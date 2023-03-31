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
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;

/** AclsWriters for writing Acls */
public class AclReaders {

    /** Read Acls for given prefix */
    public static final class SimpleAclReader implements AclReader {

        private final Admin adminClient;

        /**
         * defensive
         *
         * @param adminClient - cluster connection
         */
        private SimpleAclReader(final Admin adminClient) {
            this.adminClient = adminClient;
        }

        /**
         * Read set of acls for prefix
         *
         * @param prefix to filter against
         * @return found acls with status set to READ
         */
        @Override
        public Collection<Acl> read(final String prefix) {

            final var resourcePatternFilter =
                    new ResourcePatternFilter(ResourceType.TOPIC, prefix, PatternType.PREFIXED);

            try {
                final var aclBindingCollection =
                        adminClient
                                .describeAcls(
                                        new AclBindingFilter(
                                                resourcePatternFilter,
                                                AccessControlEntryFilter.ANY))
                                .values()
                                .get();

                return aclBindingCollection.stream()
                        .map(
                                aclBinding ->
                                        Acl.builder()
                                                .name(aclBinding.toString())
                                                .aclBinding(aclBinding)
                                                .state(Status.STATE.READ)
                                                .build())
                        .collect(Collectors.toList());
            } catch (InterruptedException | ExecutionException e) {
                throw new Provisioner.ProvisioningException("Failed to read ACLs for:" + prefix, e);
            }
        }
    }

    /** Read Acls API */
    interface AclReader {
        /**
         * Write some acls
         *
         * @param prefix to write
         * @return updated status of acls
         */
        Collection<Acl> read(String prefix);
    }

    /** TopicWriter builder */
    @SuppressFBWarnings(
            value = "EI_EXPOSE_REP2",
            justification = "adminClient() passed as param to prevent API pollution")
    public static final class AclReaderBuilder {
        private Admin adminClient;

        /** defensive */
        private AclReaderBuilder() {}

        /**
         * main builder
         *
         * @return builder
         */
        public static AclReaderBuilder builder() {
            return new AclReaderBuilder();
        }

        /**
         * add the adminClient
         *
         * @param adminClient - cluster connection
         * @return builder
         */
        public AclReaderBuilder adminClient(final Admin adminClient) {
            this.adminClient = adminClient;
            return this;
        }

        /**
         * build it
         *
         * @return the specified topic writer impl
         */
        public AclReader build() {
            return new SimpleAclReader(adminClient);
        }
    }
}
