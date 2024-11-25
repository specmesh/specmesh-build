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

import static io.specmesh.kafka.provision.Status.STATE.CREATED;
import static io.specmesh.kafka.provision.Status.STATE.DELETE;
import static io.specmesh.kafka.provision.Status.STATE.DELETED;
import static io.specmesh.kafka.provision.Status.STATE.UPDATED;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.specmesh.kafka.provision.ProvisioningException;
import io.specmesh.kafka.provision.Status.STATE;
import io.specmesh.kafka.provision.schema.SchemaProvisioner.Schema;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Mutators for mutating Schemas */
public final class SchemaMutators {

    public static final String DEFAULT_EVOLUTION = "FULL_TRANSITIVE";

    /** defensive */
    private SchemaMutators() {}

    /** Collection based */
    public static final class CollectiveMutator implements SchemaMutator {

        private final Stream<SchemaMutator> mutators;

        /**
         * iterate over the writers
         *
         * @param mutators to iterate
         */
        private CollectiveMutator(final SchemaMutator... mutators) {
            this.mutators = Arrays.stream(mutators);
        }

        /**
         * writes updates
         *
         * @param schemas to write
         * @return updated status
         */
        @Override
        public List<Schema> mutate(final Collection<Schema> schemas) {
            return this.mutators
                    .map(mutator -> mutator.mutate(schemas))
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
        }
    }

    /** Remove unspecified */
    public static final class CleanUnspecifiedMutator implements SchemaMutator {

        private final boolean dryRun;
        private final SchemaRegistryClient srClient;

        @SuppressFBWarnings(
                value = "EI_EXPOSE_REP2",
                justification = "client passed as param to prevent API pollution")
        public CleanUnspecifiedMutator(final boolean dryRun, final SchemaRegistryClient srClient) {
            this.dryRun = dryRun;
            this.srClient = srClient;
        }

        /**
         * Write updated schemas
         *
         * @param schemas to delete (maybe)
         * @return updated set of schemas
         */
        @Override
        public List<Schema> mutate(final Collection<Schema> schemas) {
            return schemas.stream()
                    .filter(
                            schema ->
                                    !schema.state().equals(STATE.UPDATE)
                                            && !schema.state().equals(STATE.CREATE)
                                            && !schema.state().equals(STATE.IGNORED))
                    .peek(
                            schema -> {
                                try {
                                    schema.state(DELETE);
                                    if (!dryRun) {
                                        final var schemaIds =
                                                srClient.deleteSubject(schema.subject());

                                        schema.state(DELETED);
                                        schema.messages(
                                                "Subject:"
                                                        + schema.subject()
                                                        + " DELETED ids: "
                                                        + schemaIds);
                                    }
                                } catch (IOException | RestClientException e) {
                                    schema.exception(
                                            new ProvisioningException(
                                                    "Failed to update schema:" + schema.subject(),
                                                    e));
                                }
                            })
                    .collect(Collectors.toList());
        }
    }

    /** Updates */
    public static final class UpdateMutator implements SchemaMutator {

        private final SchemaRegistryClient client;

        @SuppressFBWarnings(
                value = "EI_EXPOSE_REP2",
                justification = "client passed as param to prevent API pollution")
        public UpdateMutator(final SchemaRegistryClient client) {
            this.client = client;
        }

        /**
         * Write updated schemas
         *
         * @param schemas to write
         * @return updated set of schemas
         */
        @Override
        public List<Schema> mutate(final Collection<Schema> schemas) {
            return schemas.stream()
                    .filter(schema -> schema.state().equals(STATE.UPDATE))
                    .map(this::register)
                    .collect(Collectors.toList());
        }

        private Schema register(final Schema schema) {
            try {
                final var compatibilityMessages =
                        client.testCompatibilityVerbose(schema.subject(), schema.schema());

                if (!compatibilityMessages.isEmpty()) {
                    schema.messages(
                            schema.messages()
                                    + "\nCompatibility test output:"
                                    + compatibilityMessages);

                    schema.exception(
                            new ProvisioningException(
                                    "Schema compatibility issue detected for subject: "
                                            + schema.subject()));
                    return schema;
                }

                final var schemaId = client.register(schema.subject(), schema.schema());
                schema.state(UPDATED);
                schema.messages("Subject:" + schema.subject() + " Updated with id: " + schemaId);
            } catch (IOException | RestClientException e) {
                schema.exception(
                        new ProvisioningException(
                                "Failed to update schema:" + schema.subject(), e));
            }
            return schema;
        }
    }

    /** Mutate Schemas */
    public static final class WriteMutator implements SchemaMutator {

        private final SchemaRegistryClient client;

        /**
         * defensive
         *
         * @param client - cluster connection
         */
        private WriteMutator(final SchemaRegistryClient client) {
            this.client = client;
        }

        /**
         * Write the set of schemas and change status to CREATED
         *
         * @param schemas to write
         * @return set of schemas with CREATE or FAILED + Exception
         */
        @Override
        public List<Schema> mutate(final Collection<Schema> schemas) {
            return schemas.stream()
                    .filter(schema -> schema.state().equals(STATE.CREATE))
                    .peek(
                            schema -> {
                                try {
                                    final var schemaId =
                                            client.register(schema.subject(), schema.schema());
                                    client.updateCompatibility(schema.subject(), DEFAULT_EVOLUTION);
                                    schema.messages(
                                            "Subject:"
                                                    + schema.subject()
                                                    + "Created with id: "
                                                    + schemaId
                                                    + ", evolution set to:"
                                                    + DEFAULT_EVOLUTION);
                                    schema.state(CREATED);
                                } catch (IOException | RestClientException e) {
                                    schema.exception(
                                            new ProvisioningException(
                                                    "Failed to write schema:" + schema.subject(),
                                                    e));
                                }
                            })
                    .collect(Collectors.toList());
        }
    }

    /** Passes through ignored schemas so they appear in the output */
    public static final class IgnoredMutator implements SchemaMutator {

        @Override
        public List<Schema> mutate(final Collection<Schema> schemas) {

            return schemas.stream()
                    .filter(schema -> schema.state().equals(STATE.IGNORED))
                    .collect(Collectors.toList());
        }
    }

    /** Do nothing mutator */
    public static final class NoopSchemaMutator implements SchemaMutator {

        /**
         * Do nothing
         *
         * @param schemas to write
         * @return schemas without status change
         */
        @Override
        public List<Schema> mutate(final Collection<Schema> schemas) {
            return List.copyOf(schemas);
        }
    }

    /** Write schemas API */
    interface SchemaMutator {
        /**
         * Write some schemas
         *
         * @param schemas to write
         * @return updated status of schemas
         */
        List<Schema> mutate(Collection<Schema> schemas);
    }

    /**
     * brevity
     *
     * @return builder
     */
    public static SchemaMutatorBuilder builder() {
        return SchemaMutatorBuilder.builder();
    }

    /** Mutator builder */
    @SuppressFBWarnings(
            value = "EI_EXPOSE_REP2",
            justification = "adminClient() passed as param to prevent API pollution")
    public static final class SchemaMutatorBuilder {
        private SchemaRegistryClient client;
        private boolean dryRun;
        private boolean cleanUnspecified;

        /** defensive */
        private SchemaMutatorBuilder() {}

        /**
         * main builder
         *
         * @return builder
         */
        public static SchemaMutatorBuilder builder() {
            return new SchemaMutatorBuilder();
        }

        /**
         * add the adminClient
         *
         * @param client - cluster connection
         * @return builder
         */
        public SchemaMutatorBuilder schemaRegistryClient(final SchemaRegistryClient client) {
            this.client = client;
            return this;
        }

        /**
         * use a noop writer
         *
         * @param dryRun - true is dry running
         * @return the builder
         */
        public SchemaMutatorBuilder noop(final boolean dryRun) {
            this.dryRun = dryRun;
            return this;
        }

        /**
         * Clean un-specified schemas
         *
         * @param cleanUnspecified flag
         * @return mutator
         */
        public SchemaMutatorBuilder cleanUnspecified(final boolean cleanUnspecified) {
            this.cleanUnspecified = cleanUnspecified;
            return this;
        }

        /**
         * build it
         *
         * @return the specified the right kind of mutator
         */
        public SchemaMutator build() {
            if (cleanUnspecified) {
                return new CleanUnspecifiedMutator(dryRun, client);
            } else if (dryRun) {
                return new NoopSchemaMutator();
            } else {
                return new CollectiveMutator(
                        new UpdateMutator(client), new WriteMutator(client), new IgnoredMutator());
            }
        }
    }
}
