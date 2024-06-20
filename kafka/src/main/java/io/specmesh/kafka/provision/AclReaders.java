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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AclBindingFilter;

/** AclReaders for reading Acls */
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
         * Read set of acls that exist for this Spec (and those that dont) - i.e. removed
         *
         * @param specAclsBindingsNeeded to filter against
         * @return existing ACLs with status set to READ
         */
        @Override
        public Collection<Acl> read(
                final String prefixPattern, final Collection<Acl> specAclsBindingsNeeded) {

            try {
                // crappy admin client for reading 'ALL' ACLs to pickup removed ACLs
                final var allAcls =
                        adminClient
                                .describeAcls(AclBindingFilter.ANY)
                                .values()
                                .get(Provisioner.REQUEST_TIMEOUT, TimeUnit.SECONDS);

                // will include ACLs that may have been removed
                final var existingAclsSuperSet =
                        allAcls.stream()
                                .filter(
                                        aclBinding ->
                                                aclBinding
                                                        .toFilter()
                                                        .patternFilter()
                                                        .name()
                                                        .contains(prefixPattern))
                                .collect(Collectors.toSet());

                // now look for any in the spec that didnt get discovered in the 'filtering' - will
                // be 'CLUSTER' type
                final var aclNeededAsStringSet =
                        specAclsBindingsNeeded.stream()
                                .map(acl -> acl.aclBinding().toString())
                                .collect(Collectors.toSet());
                final var allAclsSpecifiedAndFound =
                        allAcls.stream()
                                .filter(anAcl -> aclNeededAsStringSet.contains(anAcl.toString()))
                                .collect(Collectors.toList());

                // add them  to the list
                existingAclsSuperSet.addAll(allAclsSpecifiedAndFound);

                return existingAclsSuperSet.stream()
                        .map(
                                aclBinding ->
                                        Acl.builder()
                                                .name(aclBinding.toString())
                                                .aclBinding(aclBinding)
                                                .state(Status.STATE.READ)
                                                .build())
                        .collect(Collectors.toList());
            } catch (Exception e) {
                if (e.getCause() != null
                        && e.getCause()
                                .toString()
                                .contains(
                                        "org.apache.kafka.common.errors.SecurityDisabledException")) {
                    return List.of();
                }
                throw new ProvisioningException("Failed to read ACLs", e);
            }
        }
    }

    /** Read Acls API */
    interface AclReader {
        /**
         * read some acls
         *
         * @param prefixPattern prefixpattern
         * @param acls to read
         * @return updated status of acls
         */
        Collection<Acl> read(String prefixPattern, Collection<Acl> acls);
    }

    /** builder */
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
         * @return the specified reader impl
         */
        public AclReader build() {
            return new SimpleAclReader(adminClient);
        }
    }
}
