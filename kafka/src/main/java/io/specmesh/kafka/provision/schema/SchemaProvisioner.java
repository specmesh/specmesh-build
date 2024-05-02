/*
 * Copyright 2023 SpecMesh Contributors (https://github.com/specmesh)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.specmesh.kafka.provision.schema;

import static io.specmesh.kafka.provision.Status.STATE.CREATE;
import static io.specmesh.kafka.provision.Status.STATE.FAILED;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.specmesh.apiparser.model.SchemaInfo;
import io.specmesh.kafka.KafkaApiSpec;
import io.specmesh.kafka.provision.ExceptionWrapper;
import io.specmesh.kafka.provision.Provisioner;
import io.specmesh.kafka.provision.Status;
import io.specmesh.kafka.provision.WithState;
import io.specmesh.kafka.provision.schema.SchemaReaders.SchemaReader;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

public final class SchemaProvisioner {

    /** defensive */
    private SchemaProvisioner() {}
    /**
     * Provision schemas to Schema Registry
     *
     * @param dryRun for mode of operation
     * @param cleanUnspecified for cleanup operations
     * @param apiSpec the api spec
     * @param baseResourcePath the path under which external schemas are stored.
     * @param client the client for the schema registry
     * @return status of actions
     */
    public static Collection<Schema> provision(
            final boolean dryRun,
            final boolean cleanUnspecified,
            final KafkaApiSpec apiSpec,
            final String baseResourcePath,
            final SchemaRegistryClient client) {

        final var reader = reader(client);

        final var existing = reader.read(apiSpec.id());

        final var required = requiredSchemas(apiSpec, baseResourcePath);

        if (required.stream().anyMatch(schema -> schema.state.equals(FAILED))) {
            throw new SchemaProvisioningException("Required Schemas Failed to load:" + required);
        }

        final var schemas = calculator(client, cleanUnspecified).calculate(existing, required);
        return mutator(dryRun, cleanUnspecified, client).mutate(schemas);
    }

    /**
     * schema writer
     *
     * @param dryRun real or noops writer
     * @param cleanUnspecified - clean unexpected resource
     * @param schemaRegistryClient - sr connection
     * @return writer instance
     */
    private static SchemaMutators.SchemaMutator mutator(
            final boolean dryRun,
            final boolean cleanUnspecified,
            final SchemaRegistryClient schemaRegistryClient) {
        return SchemaMutators.builder()
                .schemaRegistryClient(schemaRegistryClient)
                .noop(dryRun)
                .cleanUnspecified(cleanUnspecified)
                .build();
    }

    /**
     * calculator
     *
     * @return calculator
     */
    private static SchemaChangeSetCalculators.ChangeSetCalculator calculator(
            final SchemaRegistryClient client, final boolean cleanUnspecified) {
        return SchemaChangeSetCalculators.builder().build(cleanUnspecified, client);
    }

    /**
     * schemas from the api
     *
     * @param apiSpec - spec
     * @param baseResourcePath file path
     * @return list of schemas
     */
    private static List<Schema> requiredSchemas(
            final KafkaApiSpec apiSpec, final String baseResourcePath) {
        return apiSpec.listDomainOwnedTopics().stream()
                .filter(
                        topic ->
                                apiSpec.apiSpec()
                                        .channels()
                                        .get(topic.name())
                                        .publish()
                                        .isSchemaRequired())
                .map(
                        (topic -> {
                            final var schema = Schema.builder();
                            try {
                                final var schemaInfo = apiSpec.schemaInfoForTopic(topic.name());
                                final var schemaRef = schemaInfo.schemaRef();
                                final var schemaPath = Paths.get(baseResourcePath, schemaRef);
                                final var schemas =
                                        new SchemaReaders.FileSystemSchemaReader()
                                                .readLocal(schemaPath.toString());
                                schema.schemas(schemas)
                                        .type(schemaInfo.schemaRef())
                                        .subject(
                                                resolveSubjectName(
                                                        topic.name(), schemas, schemaInfo));
                                schema.state(CREATE);

                            } catch (Provisioner.ProvisioningException ex) {
                                schema.state(FAILED);
                                schema.exception(ex);
                            }
                            return schema.build();
                        }))
                .collect(Collectors.toList());
    }

    /**
     * Follow these guidelines for Confluent SR and APICurio
     * https://docs.confluent.io/platform/6.2/schema-registry/serdes-develop/index.html#referenced-schemas
     * https://docs.confluent.io/platform/6.2/schema-registry/serdes-develop/index.html#subject-name-strategy
     *
     * <p>APICurio SimpleTopicIdStrategy - Simple strategy that only uses the topic name.
     * RecordIdStrategy - Avro-specific strategy that uses the full name of the schema.
     * TopicRecordIdStrategy - Avro-specific strategy that uses the topic name and the full name of
     * the schema. TopicIdStrategy - Default strategy that uses the topic name and key or value
     * suffix.
     *
     * <p>Confluent SR TopicNameStrategy - Derives subject name from topic name. (This is the
     * default.) RecordNameStrategy - Derives subject name from record name, and provides a way to
     * group logically related events that may have different data structures under a subject.
     * TopicRecordNameStrategy - Derives the subject name from topic and record name, as a way to
     * group logically related events that may have different data structures under a subject.
     *
     * @param topicName
     * @param schemas
     * @param schemaInfo
     * @return
     */
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private static String resolveSubjectName(
            final String topicName,
            final Collection<ParsedSchema> schemas,
            final SchemaInfo schemaInfo) {
        final var lookup = schemaInfo.schemaLookupStrategy();
        if (lookup.equalsIgnoreCase("SimpleTopicIdStrategy")) {
            return topicName;
        }
        if (lookup.equalsIgnoreCase("RecordNameStrategy")
                || lookup.equalsIgnoreCase("RecordIdStrategy")) {
            final var next = schemas.iterator().next();
            if (!isAvro(next)) {
                throw new UnsupportedOperationException(
                        "Currently, only avro schemas support RecordNameStrategy and"
                                + " RecordIdStrategy");
            }
            return ((AvroSchema) next).rawSchema().getFullName();
        }
        if (lookup.equalsIgnoreCase("TopicRecordIdStrategy")
                || lookup.equalsIgnoreCase("TopicRecordNameStrategy")) {

            final var next = schemas.iterator().next();
            if (!isAvro(next)) {
                throw new UnsupportedOperationException(
                        "Currently, only avro schemas support TopicRecordNameStrategy and"
                                + " TopicRecordIdStrategy");
            }

            return topicName + "-" + ((AvroSchema) next).rawSchema().getFullName();
        }
        return topicName + "-value";
    }

    private static boolean isAvro(final ParsedSchema schema) {
        return schema.schemaType().equals("AVRO") && schema instanceof AvroSchema;
    }

    /**
     * get the reader
     *
     * @param schemaRegistryClient - sr connection
     * @return reader
     */
    private static SchemaReader reader(final SchemaRegistryClient schemaRegistryClient) {
        return SchemaReaders.builder().schemaRegistryClient(schemaRegistryClient).build();
    }

    /** Schema provisioning status */
    @Builder
    @Data
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    @Accessors(fluent = true)
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    @SuppressFBWarnings
    public static final class Schema implements WithState {
        @EqualsAndHashCode.Include private String subject;
        private Status.STATE state;
        private String type;
        private Exception exception;
        @Builder.Default private String messages = "";

        Collection<ParsedSchema> schemas;

        public Schema exception(final Exception exception) {
            this.exception = new ExceptionWrapper(exception);
            return this;
        }

        public ParsedSchema getSchema() {
            return this.schemas.iterator().next();
        }
    }

    public static class SchemaProvisioningException extends RuntimeException {

        public SchemaProvisioningException(final String msg) {
            super(msg);
        }

        public SchemaProvisioningException(final String msg, final Throwable cause) {
            super(msg, cause);
        }
    }
}
