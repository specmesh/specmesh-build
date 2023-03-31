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

import io.specmesh.kafka.provision.AclProvisioner.Acl;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Calculates a changeset of Acls to create or update, should also return incompatible changes for
 * existing topics
 */
public class AclChangeSetCalculators {

    /** Returns those topics to create and ignores existing topics */
    public static final class CreateChangeSetCalculator implements ChangeSetCalculator {

        /**
         * Calculate set of Acls that dont already exist
         *
         * @param existingAcls - existing
         * @param requiredAcls - needed
         * @return set required to create - status set to CREATE
         */
        @Override
        public Collection<Acl> calculate(
                final Collection<Acl> existingAcls, final Collection<Acl> requiredAcls) {
            return requiredAcls.stream()
                    .filter(neededAcl -> !existingAcls.contains(neededAcl))
                    .map(neededAcl -> neededAcl.state(Status.STATE.CREATE))
                    .collect(Collectors.toList());
        }
    }

    /** Main API */
    interface ChangeSetCalculator {
        /**
         * Returns changeset of topics to create/update with the 'state' flag determining which
         * actions to carry out
         *
         * @param existingAcls - existing
         * @param requiredAcls - needed
         * @return - set of those that dont exist
         */
        Collection<Acl> calculate(Collection<Acl> existingAcls, Collection<Acl> requiredAcls);
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
