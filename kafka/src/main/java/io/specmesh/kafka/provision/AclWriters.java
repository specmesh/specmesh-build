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

/** AclsWriters for writing Acls */
public class AclWriters {

    /** Writes Acls */
    public static final class SimpleAclWriter implements AclWriter {

        private final Admin adminClient;

        /**
         * defensive
         *
         * @param adminClient - cluster connection
         */
        private SimpleAclWriter(final Admin adminClient) {
            this.adminClient = adminClient;
        }

        /**
         * Write the set of acls and change status to CREATED
         *
         * @param acls to write
         * @return set of ACLs with CREATE or FAILED + Exception
         */
        @Override
        public Collection<Acl> write(final Collection<Acl> acls) {

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
                                                                .toString()));
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
                                            .toString());
                        });
            }
            return acls;
        }

        /**
         * Extract the bindingFilter for updates (so they can be deleted)
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
    public static final class NoopAclWriter implements AclWriter {

        /**
         * Do nothing
         *
         * @param acls to write
         * @return acls with status set to CREATED or FAILED
         */
        @Override
        public Collection<Acl> write(final Collection<Acl> acls) {
            return acls;
        }
    }

    /** Write Acls API */
    interface AclWriter {
        /**
         * Write some acls
         *
         * @param acls to write
         * @return updated status of acls
         */
        Collection<Acl> write(Collection<Acl> acls);
    }

    /** TopicWriter builder */
    @SuppressFBWarnings(
            value = "EI_EXPOSE_REP2",
            justification = "adminClient() passed as param to prevent API pollution")
    public static final class AclWriterBuilder {
        private Admin adminClient;
        private boolean noopWriter;

        /** defensive */
        private AclWriterBuilder() {}

        /**
         * main builder
         *
         * @return builder
         */
        public static AclWriterBuilder builder() {
            return new AclWriterBuilder();
        }

        /**
         * add the adminClient
         *
         * @param adminClient - cluster connection
         * @return builder
         */
        public AclWriterBuilder adminClient(final Admin adminClient) {
            this.adminClient = adminClient;
            return this;
        }

        /**
         * use a noop writer
         *
         * @param dryRun - true is dry running
         * @return the builder
         */
        public AclWriterBuilder noop(final boolean dryRun) {
            noopWriter = dryRun;
            return this;
        }

        /**
         * build it
         *
         * @return the specified topic writer impl
         */
        public AclWriter build() {
            if (noopWriter) {
                return new NoopAclWriter();
            } else {
                return new SimpleAclWriter(adminClient);
            }
        }
    }
}
