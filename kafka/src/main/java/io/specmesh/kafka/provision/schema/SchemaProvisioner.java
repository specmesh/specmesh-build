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
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.specmesh.kafka.KafkaApiSpec;
import io.specmesh.kafka.provision.ExceptionWrapper;
import io.specmesh.kafka.provision.Provisioner;
import io.specmesh.kafka.provision.Status;
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
                                final var schemaSubject = topic.name() + "-value";
                                final var schemaInfo = apiSpec.schemaInfoForTopic(topic.name());
                                final var schemaRef = schemaInfo.schemaRef();
                                final var schemaPath = Paths.get(baseResourcePath, schemaRef);
                                schema.schemas(
                                                new SchemaReaders.FileSystemSchemaReader()
                                                        .readLocal(schemaPath.toString()))
                                        .type(schemaInfo.schemaRef())
                                        .subject(schemaSubject);
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
    public static class Schema {
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
