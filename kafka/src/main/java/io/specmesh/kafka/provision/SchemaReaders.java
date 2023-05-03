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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.specmesh.kafka.provision.SchemaProvisioner.Schema;
import java.io.IOException;
import java.util.Collection;
import java.util.stream.Collectors;

/** Readers for reading Schemas */
public final class SchemaReaders {

    /** defensive */
    private SchemaReaders() {}

    /** Read Acls for given prefix */
    public static final class SimpleSchemaReader implements SchemaReader {

        private final SchemaRegistryClient client;

        /**
         * defensive
         *
         * @param client - cluster connection
         */
        private SimpleSchemaReader(final SchemaRegistryClient client) {
            this.client = client;
        }

        /**
         * Read set of schemas for subject
         *
         * @param prefix to filter against
         * @return found acls with status set to READ
         */
        @Override
        public Collection<Schema> read(final String prefix) {

            try {
                final var subjects = client.getAllSubjectsByPrefix(prefix);
                final var schemas =
                        subjects.stream()
                                .collect(
                                        Collectors.toMap(
                                                subject -> subject,
                                                subject -> {
                                                    try {
                                                        return client.getSchemas(
                                                                subject, false, true);
                                                    } catch (IOException | RestClientException e) {
                                                        throw new Provisioner.ProvisioningException(
                                                                "Failed to load schemas", e);
                                                    }
                                                }));

                return schemas.entrySet().stream()
                        .filter(entry -> !entry.getValue().isEmpty())
                        .map(
                                entry ->
                                        Schema.builder()
                                                .subject(entry.getKey())
                                                .type(entry.getValue().get(0).schemaType())
                                                .payload(entry.getValue().get(0).canonicalString())
                                                .state(Status.STATE.READ)
                                                .build())
                        .collect(Collectors.toList());
            } catch (RestClientException | IOException e) {
                throw new Provisioner.ProvisioningException(
                        "Failed to read schemas for:" + prefix, e);
            }
        }
    }

    /** Read Acls API */
    interface SchemaReader {
        /**
         * read some acls
         *
         * @param prefix to read
         * @return updated status of acls
         */
        Collection<Schema> read(String prefix);
    }

    /**
     * berevity
     *
     * @return builder
     */
    public static AclReaderBuilder builder() {
        return AclReaderBuilder.builder();
    }

    /** builder */
    @SuppressFBWarnings(
            value = "EI_EXPOSE_REP2",
            justification = "adminClient() passed as param to prevent API pollution")
    public static final class AclReaderBuilder {

        private SchemaRegistryClient schemaRegistryClient;

        /** defensive */
        private AclReaderBuilder() {}

        /**
         * main builder
         *
         * @return builder
         */
        public static AclReaderBuilder builder() {
            return new AclReaderBuilder();
        }

        public AclReaderBuilder schemaRegistryClient(
                final SchemaRegistryClient schemaRegistryClient) {
            this.schemaRegistryClient = schemaRegistryClient;
            return this;
        }

        /**
         * build it
         *
         * @return the specified reader impl
         */
        public SchemaReader build() {
            return new SimpleSchemaReader(schemaRegistryClient);
        }
    }
}
