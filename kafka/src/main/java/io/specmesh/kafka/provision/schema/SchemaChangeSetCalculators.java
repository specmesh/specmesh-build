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

import static java.util.Objects.requireNonNull;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.specmesh.kafka.provision.Status;
import io.specmesh.kafka.provision.schema.SchemaProvisioner.Schema;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Calculates a changeset of Schemas to create or update, should also return incompatible changes
 * for existing schemas
 */
final class SchemaChangeSetCalculators {

    /** defensive */
    private SchemaChangeSetCalculators() {}

    /** Collection based */
    static final class Collective implements ChangeSetCalculator {

        private final IgnorePreprocessor preprocessor;
        private final List<ChangeSetCalculator> calculatorStream;

        private Collective(
                final IgnorePreprocessor preprocessor, final ChangeSetCalculator... writers) {
            this.preprocessor = requireNonNull(preprocessor, "preprocessor");
            this.calculatorStream = List.of(writers);
        }

        @Override
        public Collection<Schema> calculate(
                final Collection<Schema> existing,
                final Collection<Schema> required,
                final String domainId) {
            final Collection<Schema> preprocessed = preprocessor.calculate(required, domainId);
            return calculatorStream.stream()
                    .map(calculator -> calculator.calculate(existing, preprocessed, domainId))
                    .flatMap(Collection::stream)
                    .toList();
        }
    }

    /** Return set of 'unspecific' (i.e. non-required) schemas */
    static class CleanUnspecifiedCalculator implements ChangeSetCalculator {

        @Override
        public Collection<Schema> calculate(
                final Collection<Schema> existing,
                final Collection<Schema> required,
                final String domainId) {
            existing.removeAll(required);
            return existing;
        }
    }

    /** Returns those schemas to create and ignores existing */
    static final class UpdateCalculator implements ChangeSetCalculator {

        @Override
        public Collection<Schema> calculate(
                final Collection<Schema> existing,
                final Collection<Schema> required,
                final String domainId) {
            final var existingList = new ArrayList<>(existing);
            return required.stream()
                    .filter(needs -> hasChanged(needs, existingList))
                    .peek(
                            schema ->
                                    schema.messages(schema.messages() + "\n Update")
                                            .state(Status.STATE.UPDATE))
                    .toList();
        }

        private boolean hasChanged(final Schema needs, final List<Schema> existingList) {
            final var foundAt = existingList.indexOf(needs);
            if (foundAt != -1) {
                final var existing = existingList.get(foundAt);
                return !normalizeSchema(existing.schema()).equals(normalizeSchema(needs.schema()));
            } else {
                return false;
            }
        }

        private ParsedSchema normalizeSchema(final ParsedSchema schema) {
            if (!(schema instanceof AvroSchema)) {
                // References not yet supported:
                return schema.normalize();
            }

            final AvroSchema avroSchema = (AvroSchema) schema.normalize();

            final List<SchemaReference> references =
                    avroSchema.references().stream()
                            .map(ref -> new SchemaReference(ref.getName(), ref.getSubject(), -1))
                            .toList();

            return new AvroSchema(
                    avroSchema.canonicalString(),
                    references,
                    avroSchema.resolvedReferences(),
                    avroSchema.metadata(),
                    avroSchema.ruleSet(),
                    -1,
                    false);
        }
    }

    /** Returns those schemas to create and ignores existing */
    static final class CreateCalculator implements ChangeSetCalculator {

        @Override
        public Collection<Schema> calculate(
                final Collection<Schema> existing,
                final Collection<Schema> required,
                final String domainId) {
            return required.stream()
                    .filter(schema -> needsToBeCreated(schema, existing, domainId))
                    .map(schema -> schema.state(Status.STATE.CREATE))
                    .peek(schema -> schema.messages(schema.messages() + "\n Create"))
                    .toList();
        }

        private static boolean needsToBeCreated(
                final Schema schema, final Collection<Schema> existing, final String domainId) {
            if (!schema.state().equals(Status.STATE.READ)
                    && !schema.state().equals(Status.STATE.CREATE)) {
                return false;
            }
            if (!schema.topicSchema() && !SchemaOwnership.schemaOwnedByDomain(schema, domainId)) {
                return false;
            }

            return !existing.contains(schema);
        }
    }

    /** Ignores schemas from outside the domain */
    static final class IgnorePreprocessor {

        public Collection<Schema> calculate(
                final Collection<Schema> required, final String domainId) {
            return required.stream()
                    .map(schema -> markIgnoredIfOutsideDomain(schema, domainId))
                    .toList();
        }

        private Schema markIgnoredIfOutsideDomain(final Schema schema, final String domainId) {
            if (!schema.topicSchema() && !SchemaOwnership.schemaOwnedByDomain(schema, domainId)) {
                return schema.state(Status.STATE.IGNORED)
                        .messages("\n ignored as it does not belong to the domain");
            }

            return schema;
        }
    }

    /** Allows ignored schema to flow through to response. */
    static final class IgnoreCalculator implements ChangeSetCalculator {

        @Override
        public Collection<Schema> calculate(
                final Collection<Schema> existing,
                final Collection<Schema> required,
                final String domainId) {
            return required.stream()
                    .filter(schema -> schema.state().equals(Status.STATE.IGNORED))
                    .toList();
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
         * @param domainId the id of the domain being provisioned
         * @return - set of those that don't exist
         */
        Collection<Schema> calculate(
                Collection<Schema> existing, Collection<Schema> required, String domainId);
    }

    /**
     * brevity
     *
     * @return builder
     */
    static ChangeSetBuilder builder() {
        return ChangeSetBuilder.builder();
    }

    /** Builder of the things */
    static final class ChangeSetBuilder {

        /** defensive */
        private ChangeSetBuilder() {}

        /**
         * protected method
         *
         * @return builder
         */
        static ChangeSetBuilder builder() {
            return new ChangeSetBuilder();
        }

        /**
         * build it
         *
         * @param cleanUnspecified - cleanup
         * @return required calculator
         */
        ChangeSetCalculator build(final boolean cleanUnspecified) {
            if (cleanUnspecified) {
                return new CleanUnspecifiedCalculator();

            } else {
                return new Collective(
                        new IgnorePreprocessor(),
                        new IgnoreCalculator(),
                        new UpdateCalculator(),
                        new CreateCalculator());
            }
        }
    }
}
