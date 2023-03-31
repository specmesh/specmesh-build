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
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;

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

            final var aclBindings = acls.stream().map(Acl::aclBinding).collect(Collectors.toList());

            try {
                adminClient
                        .createAcls(aclBindings)
                        .all()
                        .get(Provisioner.REQUEST_TIMEOUT, TimeUnit.SECONDS);
                acls.forEach(acl -> acl.state(Status.STATE.CREATED));
            } catch (Exception ex) {
                acls.forEach(
                        acl -> {
                            acl.state(Status.STATE.FAILED);
                            acl.exception(
                                    new Provisioner.ProvisioningException(
                                            "Failed to provision set of Acls", ex));
                        });
            }
            return acls;
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
