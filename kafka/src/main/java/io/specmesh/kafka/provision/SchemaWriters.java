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

import static io.specmesh.kafka.provision.Status.STATE.CREATED;
import static io.specmesh.kafka.provision.Status.STATE.FAILED;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.specmesh.kafka.provision.SchemaProvisioner.Schema;
import io.specmesh.kafka.provision.Status.STATE;
import java.io.IOException;
import java.util.Collection;
import java.util.stream.Collectors;

/** Writers for writing Schemas */
public final class SchemaWriters {

    /** defensive */
    private SchemaWriters() {}

    /** Writes Schemas */
    public static final class SimpleWriter implements SchemaWriter {

        private final SchemaRegistryClient client;

        /**
         * defensive
         *
         * @param client - cluster connection
         */
        private SimpleWriter(final SchemaRegistryClient client) {
            this.client = client;
        }

        /**
         * Write the set of schemas and change status to CREATED
         *
         * @param schemas to write
         * @return set of schemas with CREATE or FAILED + Exception
         */
        @Override
        public Collection<Schema> write(final Collection<Schema> schemas) {

            return schemas.stream()
                    .filter(schema -> schema.state().equals(STATE.CREATE))
                    .peek(
                            schema -> {
                                final var parsedSchema = getSchema(schema.type(), schema.payload());
                                try {
                                    final var schemaId =
                                            client.register(schema.subject(), parsedSchema);
                                    schema.message("Created with id: " + schemaId);
                                    schema.state(CREATED);
                                } catch (IOException | RestClientException e) {
                                    schema.exception(
                                            new Provisioner.ProvisioningException(
                                                    "Failed to write schema:" + schema.subject(),
                                                    e));
                                    schema.state(FAILED);
                                }
                            })
                    .collect(Collectors.toList());
        }

        static ParsedSchema getSchema(final String schemaRefType, final String content) {

            if (schemaRefType.endsWith(".avsc")) {
                return new AvroSchema(content);
            }
            if (schemaRefType.endsWith(".yml")) {
                return new JsonSchema(content);
            }
            if (schemaRefType.endsWith(".proto")) {
                return new ProtobufSchema(content);
            }
            throw new Provisioner.ProvisioningException("Unsupported schema type");
        }
    }

    /** Do nothing writer */
    public static final class NoopSchemaWriter implements SchemaWriter {

        /**
         * Do nothing
         *
         * @param schemas to write
         * @return schemas without status change
         */
        @Override
        public Collection<Schema> write(final Collection<Schema> schemas) {
            return schemas;
        }
    }

    /** Write schemas API */
    interface SchemaWriter {
        /**
         * Write some schemas
         *
         * @param schemas to write
         * @return updated status of schemas
         */
        Collection<Schema> write(Collection<Schema> schemas);
    }

    /**
     * brevity
     *
     * @return builder
     */
    public static SchemaWriterBuilder builder() {
        return SchemaWriterBuilder.builder();
    }

    /** TopicWriter builder */
    @SuppressFBWarnings(
            value = "EI_EXPOSE_REP2",
            justification = "adminClient() passed as param to prevent API pollution")
    public static final class SchemaWriterBuilder {
        private SchemaRegistryClient client;
        private boolean noopWriter;

        /** defensive */
        private SchemaWriterBuilder() {}

        /**
         * main builder
         *
         * @return builder
         */
        public static SchemaWriterBuilder builder() {
            return new SchemaWriterBuilder();
        }

        /**
         * add the adminClient
         *
         * @param client - cluster connection
         * @return builder
         */
        public SchemaWriterBuilder schemaRegistryClient(final SchemaRegistryClient client) {
            this.client = client;
            return this;
        }

        /**
         * use a noop writer
         *
         * @param dryRun - true is dry running
         * @return the builder
         */
        public SchemaWriterBuilder noop(final boolean dryRun) {
            noopWriter = dryRun;
            return this;
        }

        /**
         * build it
         *
         * @return the specified topic writer impl
         */
        public SchemaWriter build() {
            if (noopWriter) {
                return new NoopSchemaWriter();
            } else {
                return new SimpleWriter(client);
            }
        }
    }
}
