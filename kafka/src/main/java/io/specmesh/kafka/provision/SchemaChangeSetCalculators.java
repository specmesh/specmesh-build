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

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.specmesh.kafka.provision.SchemaProvisioner.Schema;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Calculates a changeset of Schemas to create or update, should also return incompatible changes
 * for existing schemas
 */
public final class SchemaChangeSetCalculators {

    /** defensive */
    private SchemaChangeSetCalculators() {}

    /** Collection based */
    public static final class Collective implements ChangeSetCalculator {

        private final Stream<ChangeSetCalculator> calculatorStream;

        /**
         * iterate over the calculators
         *
         * @param writers to iterate
         */
        private Collective(final ChangeSetCalculator... writers) {
            this.calculatorStream = Arrays.stream(writers);
        }

        /**
         * delegates updates
         *
         * @param existing schemas
         * @param required needed schemas
         * @return updated status
         */
        @Override
        public Collection<Schema> calculate(
                final Collection<Schema> existing, final Collection<Schema> required) {
            return this.calculatorStream
                    .map(calculator -> calculator.calculate(existing, required))
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
        }
    }

    /** Returns those schemas to create and ignores existing */
    public static final class UpdateCalculator implements ChangeSetCalculator {

        private final SchemaRegistryClient client;

        private UpdateCalculator(final SchemaRegistryClient client) {
            this.client = client;
        }

        /**
         * Calculate the set of schemas to Update and also checks compatibility
         *
         * @param existing - existing
         * @param required - needed
         * @return updated set of schemas
         */
        @Override
        public Collection<Schema> calculate(
                final Collection<Schema> existing, final Collection<Schema> required) {
            return required.stream()
                    .filter(existing::contains)
                    .peek(
                            schema -> {
                                schema.messages(schema.messages() + "\n Update");
                                try {
                                    final var parsedSchema =
                                            getSchema(schema.type(), schema.payload());
                                    final var compatibilityMessages =
                                            client.testCompatibilityVerbose(
                                                    schema.subject(), parsedSchema);
                                    schema.messages(
                                            schema.messages()
                                                    + "\nCompatibility test output:"
                                                    + compatibilityMessages);
                                    if (!compatibilityMessages.isEmpty()) {
                                        schema.state(Status.STATE.FAILED);
                                    } else {
                                        schema.state(Status.STATE.UPDATE);
                                    }

                                } catch (IOException | RestClientException ex) {
                                    schema.state(Status.STATE.FAILED);
                                    schema.messages(schema.messages() + "\nException:" + ex);
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

    /** Returns those schemas to create and ignores existing */
    public static final class CreateCalculator implements ChangeSetCalculator {

        /**
         * Calculate set of schemas that dont already exist
         *
         * @param existing - existing schemas - state READ
         * @param required - needed schemas - state CREATE
         * @return set required to create - status set to CREATE
         */
        @Override
        public Collection<Schema> calculate(
                final Collection<Schema> existing, final Collection<Schema> required) {
            return required.stream()
                    .filter(
                            schema ->
                                    !existing.contains(schema)
                                            && (schema.state().equals(Status.STATE.READ)
                                                    || schema.state().equals(Status.STATE.CREATE)))
                    .map(schema -> schema.state(Status.STATE.CREATE))
                    .peek(schema -> schema.messages(schema.messages() + "\n Create"))
                    .collect(Collectors.toList());
        }
    }

    /** Main API */
    interface ChangeSetCalculator {
        /**
         * Returns changeset of schemas to create/update with the 'state' flag determining which
         * actions to carry out
         *
         * @param existing - existing
         * @param required - needed
         * @return - set of those that dont exist
         */
        Collection<Schema> calculate(Collection<Schema> existing, Collection<Schema> required);
    }

    /**
     * brevity
     *
     * @return builder
     */
    public static ChangeSetBuilder builder() {
        return ChangeSetBuilder.builder();
    }

    /** Builder of the things */
    public static final class ChangeSetBuilder {

        /** defensive */
        private ChangeSetBuilder() {}

        /**
         * protected method
         *
         * @return builder
         */
        public static ChangeSetBuilder builder() {
            return new ChangeSetBuilder();
        }

        /**
         * build it
         *
         * @param client sr client
         * @return required calculator
         */
        public ChangeSetCalculator build(final SchemaRegistryClient client) {
            return new Collective(new UpdateCalculator(client), new CreateCalculator());
        }
    }
}
