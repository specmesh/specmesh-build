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
import io.specmesh.kafka.provision.AclProvisioner.Acl;
import io.specmesh.kafka.provision.Provisioner.ProvisioningException;
import io.specmesh.kafka.provision.Status.STATE;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AclBindingFilter;

/** Mutant Acls */
public class AclMutators {

    /** Writes Acls */
    public static final class AclUnspecCleaner implements AclMutator {

        private final Admin adminClient;
        private final boolean dryRun;

        /**
         * defensive
         *
         * @param adminClient - cluster connection
         * @param dryRun - dryRun
         */
        private AclUnspecCleaner(final Admin adminClient, final boolean dryRun) {
            this.adminClient = adminClient;
            this.dryRun = dryRun;
        }

        /**
         * Write the set of acls and change status to CREATED
         *
         * @param acls to write
         * @return set of ACLs with CREATE or FAILED + Exception
         */
        @Override
        public Collection<Acl> mutate(final Collection<Acl> acls) {

            final var aclsToDelete =
                    acls.stream()
                            .filter(
                                    acl ->
                                            !acl.state().equals(STATE.CREATE)
                                                    && !acl.state().equals(STATE.UPDATE))
                            .collect(Collectors.toList());

            final var aclBindingFilters =
                    aclsToDelete.stream()
                            .map(acl -> acl.aclBinding().toFilter())
                            .collect(Collectors.toList());

            try {
                aclsToDelete.forEach(acl -> acl.state(STATE.DELETE));
                if (!dryRun) {
                    adminClient.deleteAcls(aclBindingFilters);
                    aclsToDelete.forEach(acl -> acl.state(STATE.DELETED));
                }
            } catch (Exception ex) {
                acls.stream()
                        .peek(
                                acl ->
                                        acl.state(STATE.FAILED)
                                                .messages(
                                                        acl.messages() + "\n Failed to delete ACLs")
                                                .exception(
                                                        new ProvisioningException(
                                                                        "Failed to delete ACL", ex)
                                                                ));
                return acls;
            }
            return aclsToDelete;
        }
    }

    /** Writes Acls */
    public static final class AclWriter implements AclMutator {

        private final Admin adminClient;

        /**
         * defensive
         *
         * @param adminClient - cluster connection
         */
        private AclWriter(final Admin adminClient) {
            this.adminClient = adminClient;
        }

        /**
         * Write the set of acls and change status to CREATED
         *
         * @param acls to write
         * @return set of ACLs with CREATE or FAILED + Exception
         */
        @Override
        public Collection<Acl> mutate(final Collection<Acl> acls) {

            if (deleteAclsInPrepForUpdate(acls)) {
                return acls;
            }

            final var createAcls =
                    acls.stream()
                            .filter(
                                    acl ->
                                            acl.state().equals(STATE.CREATE)
                                                    || acl.state().equals(STATE.UPDATE))
                            .collect(Collectors.toList());
            final var bindingsToCreate =
                    createAcls.stream().map(Acl::aclBinding).collect(Collectors.toList());

            try {
                adminClient
                        .createAcls(bindingsToCreate)
                        .all()
                        .get(Provisioner.REQUEST_TIMEOUT, TimeUnit.SECONDS);
                createAcls.forEach(acl -> acl.state(STATE.CREATED));
            } catch (Exception ex) {
                createAcls.forEach(
                        acl -> {
                            acl.state(STATE.FAILED);
                            acl.exception(
                                    new ProvisioningException("Failed to provision set of Acls", ex)
                                            );
                        });
            }
            return acls;
        }

        private boolean deleteAclsInPrepForUpdate(final Collection<Acl> acls) {
            final var updateBindingsFilters = bindingFiltersForUpdates(acls);
            try {
                adminClient.deleteAcls(updateBindingsFilters);
            } catch (Exception ex) {
                acls.stream()
                        .filter(acl -> acl.state().equals(STATE.UPDATE))
                        .peek(
                                acl ->
                                        acl.state(STATE.FAILED)
                                                .messages(
                                                        acl.messages() + "\n Failed to delete ACLs")
                                                .exception(
                                                        new ProvisioningException(
                                                                        "Failed to delete ACL", ex)
                                                                ));
                return true;
            }
            return false;
        }

        /**
         * Extract the bindingFilter for updates - need to deleted first
         *
         * @param acls to filter
         * @return bindings
         */
        private Collection<AclBindingFilter> bindingFiltersForUpdates(final Collection<Acl> acls) {
            return acls.stream()
                    .filter(acl -> acl.state().equals(STATE.UPDATE))
                    .map(acl -> acl.aclBinding().toFilter())
                    .collect(Collectors.toList());
        }
    }

    /** Do nothing writer */
    public static final class NoopAclMutator implements AclMutator {

        /**
         * Do nothing
         *
         * @param acls to write
         * @return acls with status set to CREATED or FAILED
         */
        @Override
        public Collection<Acl> mutate(final Collection<Acl> acls) {
            return acls;
        }
    }

    /** Write Acls API */
    interface AclMutator {
        /**
         * Write some acls
         *
         * @param acls to write
         * @return updated status of acls
         */
        Collection<Acl> mutate(Collection<Acl> acls);
    }

    /** Mutator builder */
    @SuppressFBWarnings(
            value = "EI_EXPOSE_REP2",
            justification = "adminClient() passed as param to prevent API pollution")
    public static final class AclMutatorBuilder {
        private Admin adminClient;
        private boolean dryRun;
        private boolean cleanUnspecified;

        /** defensive */
        private AclMutatorBuilder() {}

        /**
         * main builder
         *
         * @return builder
         */
        public static AclMutatorBuilder builder() {
            return new AclMutatorBuilder();
        }

        /**
         * add the adminClient
         *
         * @param adminClient - cluster connection
         * @return builder
         */
        public AclMutatorBuilder adminClient(final Admin adminClient) {
            this.adminClient = adminClient;
            return this;
        }

        /**
         * use a noop
         *
         * @param dryRun - true is dry running
         * @return the builder
         */
        public AclMutatorBuilder noop(final boolean dryRun) {
            this.dryRun = dryRun;
            return this;
        }

        public AclMutatorBuilder unspecified(final boolean cleanUnspecified) {
            this.cleanUnspecified = cleanUnspecified;
            return this;
        }

        /**
         * build it
         *
         * @return the required mutator impl
         */
        public AclMutator build() {
            if (cleanUnspecified) {
                return new AclUnspecCleaner(adminClient, dryRun);
            }
            if (dryRun) {
                return new NoopAclMutator();
            } else {
                return new AclWriter(adminClient);
            }
        }
    }
}
