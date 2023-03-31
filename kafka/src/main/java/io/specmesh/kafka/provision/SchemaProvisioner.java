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

package io.specmesh.kafka.provision;

import static io.specmesh.kafka.provision.Status.STATE.FAILED;
import static io.specmesh.kafka.provision.Status.STATE.READ;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.specmesh.kafka.KafkaApiSpec;
import io.specmesh.kafka.provision.SchemaReaders.SchemaReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

public final class SchemaProvisioner {

    /** defensive */
    private SchemaProvisioner() {}
    /**
     * Provision schemas to Schema Registry
     *
     * @param dryRun for mode of operation
     * @param apiSpec the api spec
     * @param baseResourcePath the path under which external schemas are stored.
     * @param client the client for the schema registry
     * @return status of actions
     */
    public static Collection<Schema> provision(
            final boolean dryRun,
            final KafkaApiSpec apiSpec,
            final String baseResourcePath,
            final SchemaRegistryClient client) {

        final var reader = reader(client);

        final var existing = reader.read(apiSpec.id());

        final var required = requiredSchemas(apiSpec, baseResourcePath);

        return writer(dryRun, client).write(calculator().calculate(existing, required));
    }

    /**
     * schema writer
     *
     * @param dryRun real or noops writer
     * @param schemaRegistryClient - sr connection
     * @return writer instance
     */
    private static SchemaWriters.SchemaWriter writer(
            final boolean dryRun, final SchemaRegistryClient schemaRegistryClient) {
        return SchemaWriters.builder()
                .schemaRegistryClient(schemaRegistryClient)
                .noop(dryRun)
                .build();
    }

    /**
     * calculator
     *
     * @return calculator
     */
    private static SchemaChangeSetCalculators.ChangeSetCalculator calculator() {
        return SchemaChangeSetCalculators.builder().build();
    }

    /**
     * schemas from the api
     *
     * @param apiSpec - spec
     * @param baseResourcePath file path
     * @return set of schemas
     */
    private static List<Schema> requiredSchemas(
            final KafkaApiSpec apiSpec, final String baseResourcePath) {
        return apiSpec.listDomainOwnedTopics().stream()
                .map(
                        (topic -> {
                            final var schema = Schema.builder();
                            try {
                                final var schemaSubject = topic.name() + "-value";
                                final var schemaInfo = apiSpec.schemaInfoForTopic(topic.name());
                                final var schemaRef = schemaInfo.schemaRef();
                                final var schemaPath = Paths.get(baseResourcePath, schemaRef);
                                schema.type(schemaInfo.schemaRef())
                                        .subject(schemaSubject)
                                        .payload(readSchemaContent(schemaPath));
                                schema.state(READ);

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

    static String readSchemaContent(final Path schemaPath) {
        final String schemaContent;
        try {
            schemaContent = Files.readString(schemaPath, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new Provisioner.ProvisioningException(
                    "Failed to read schema from: " + schemaPath, e);
        }
        return schemaContent;
    }

    /** Schema provisioning status */
    @Builder
    @Data
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    @Accessors(fluent = true)
    @SuppressFBWarnings
    public static class Schema {
        private Status.STATE state;
        private String type;
        private String subject;
        private String payload;
        private Exception exception;
        private String message;
    }
}
