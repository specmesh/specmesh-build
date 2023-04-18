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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Calculates a changeset of Acls to create or update, should also return incompatible changes for
 * existing acls
 */
public class AclChangeSetCalculators {

    /** Returns those acls to create and ignores existing */
    public static final class CreateOrUpdateCalculator implements ChangeSetCalculator {

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

            final var createAcls =
                    requiredAcls.stream()
                            .filter(neededAcl -> !existingAcls.contains(neededAcl))
                            .map(neededAcl -> neededAcl.state(Status.STATE.CREATE))
                            .collect(Collectors.toList());

            final var existingAcls1 = new ArrayList<>(existingAcls);
            createAcls.addAll(
                    requiredAcls.stream()
                            .filter(neededAcl -> hasChanged(neededAcl, existingAcls1))
                            .map(neededAcl -> neededAcl.state(Status.STATE.UPDATE))
                            .collect(Collectors.toList()));

            return createAcls;
        }

        /**
         * Is it different?
         *
         * @param neededAcl - need this one
         * @param existingAcls - but has this
         * @return true if it is different
         */
        private boolean hasChanged(final Acl neededAcl, final List<Acl> existingAcls) {
            final var indexOfExisting = existingAcls.indexOf(neededAcl);
            if (indexOfExisting != -1) {
                return !existingAcls
                        .get(indexOfExisting)
                        .aclBinding()
                        .equals(neededAcl.aclBinding());
            }
            return false;
        }
    }

    /** Main API */
    interface ChangeSetCalculator {
        /**
         * Returns changeset of acls to create/update with the 'state' flag determining which
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
            return new CreateOrUpdateCalculator();
        }
    }
}
