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

import io.specmesh.kafka.provision.SchemaProvisioner.Schema;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Calculates a changeset of Schemas to create or update, should also return incompatible changes
 * for existing schemas
 */
public final class SchemaChangeSetCalculators {

    /** defensive */
    private SchemaChangeSetCalculators() {}

    /** Returns those schemas to create and ignores existing */
    public static final class CreateChangeSetCalculator implements ChangeSetCalculator {

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
         * @return required calculator
         */
        public ChangeSetCalculator build() {
            return new CreateChangeSetCalculator();
        }
    }
}
