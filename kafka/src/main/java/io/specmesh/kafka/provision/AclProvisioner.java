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
import io.specmesh.kafka.KafkaApiSpec;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AclBinding;

public final class AclProvisioner {

    /** defensive */
    private AclProvisioner() {}
    /**
     * Provision acls in the Kafka cluster
     *
     * @param dryRun for mode of operation
     * @param apiSpec respect the spec
     * @param adminClient cluster connection
     * @return status of provisioning
     * @throws Provisioner.ProvisioningException on interrupt
     */
    public static Collection<Acl> provision(
            final boolean dryRun, final KafkaApiSpec apiSpec, final Admin adminClient) {

        final var requiredAcls = apiSpec.requiredAcls();
        final var existing = reader(adminClient).read(requiredAcls);

        final var required = calculator().calculate(existing, bindingsToAcls(requiredAcls));

        return writer(dryRun, adminClient).write(required);
    }

    /**
     * changeset calculator
     *
     * @return calculator
     */
    private static AclChangeSetCalculators.ChangeSetCalculator calculator() {
        return AclChangeSetCalculators.ChangeSetBuilder.builder().build();
    }

    /**
     * acl reader
     *
     * @param adminClient - cluster connection
     * @return reader inastance
     */
    private static AclReaders.AclReader reader(final Admin adminClient) {
        return AclReaders.AclReaderBuilder.builder().adminClient(adminClient).build();
    }

    /**
     * acl writer
     *
     * @param dryRun - real or not
     * @param adminClient - cluster connection
     * @return - writer instance
     */
    private static AclWriters.AclWriter writer(final boolean dryRun, final Admin adminClient) {
        return AclWriters.AclWriterBuilder.builder().noop(dryRun).adminClient(adminClient).build();
    }

    /**
     * convert bindings
     *
     * @param allAcls bindings to convert
     * @return - converted set
     */
    private static List<Acl> bindingsToAcls(final Set<AclBinding> allAcls) {
        return allAcls.stream()
                .map(
                        aclBinding ->
                                Acl.builder()
                                        .aclBinding(aclBinding)
                                        .name(aclBinding.toString())
                                        .state(Status.STATE.READ)
                                        .build())
                .collect(Collectors.toList());
    }

    /** Acls provisioning status */
    @Builder
    @Data
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    @Accessors(fluent = true)
    @SuppressFBWarnings
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    public static class Acl {
        @EqualsAndHashCode.Include private String name;
        private Status.STATE state;
        private AclBinding aclBinding;
        private String exception;
        @Builder.Default private String messages = "";
    }
}
