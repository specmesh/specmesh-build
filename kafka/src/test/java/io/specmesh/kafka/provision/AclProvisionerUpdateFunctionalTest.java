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

import static org.apache.kafka.common.acl.AclOperation.ALL;
import static org.apache.kafka.common.acl.AclOperation.IDEMPOTENT_WRITE;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.common.resource.PatternType.LITERAL;
import static org.apache.kafka.common.resource.PatternType.PREFIXED;
import static org.apache.kafka.common.resource.Resource.CLUSTER_NAME;
import static org.apache.kafka.common.resource.ResourceType.CLUSTER;
import static org.apache.kafka.common.resource.ResourceType.GROUP;
import static org.apache.kafka.common.resource.ResourceType.TRANSACTIONAL_ID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.specmesh.kafka.DockerKafkaEnvironment;
import io.specmesh.kafka.KafkaApiSpec;
import io.specmesh.kafka.KafkaEnvironment;
import io.specmesh.kafka.provision.Status.STATE;
import io.specmesh.test.TestSpecLoader;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Tests execution DryRuns and UPDATES where the provisioner-functional-test-api.yml is already
 * provisioned
 */
@SuppressFBWarnings(
        value = "IC_INIT_CIRCULARITY",
        justification = "shouldHaveInitializedEnumsCorrectly() proves this is false positive")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class AclProvisionerUpdateFunctionalTest {

    private static final KafkaApiSpec API_SPEC =
            TestSpecLoader.loadFromClassPath("provisioner-functional-test-api.yaml");

    private static final KafkaApiSpec API_UPDATE_SPEC =
            TestSpecLoader.loadFromClassPath("provisioner-update-functional-test-api.yaml");

    private enum Domain {
        /** The domain associated with the spec. */
        SELF(API_SPEC.id()),
        /** An unrelated domain. */
        UNRELATED("london.hammersmith.transport"),
        /** A domain granted access to the protected topic. */
        LIMITED("some.other.domain.root");

        final String domainId;

        Domain(final String name) {
            this.domainId = name;
        }
    }

    private static final String ADMIN_USER = "admin";

    @RegisterExtension
    private static final KafkaEnvironment KAFKA_ENV =
            DockerKafkaEnvironment.builder()
                    .withSaslAuthentication(
                            ADMIN_USER,
                            ADMIN_USER + "-secret",
                            Domain.SELF.domainId,
                            Domain.SELF.domainId + "-secret")
                    .withKafkaAcls(aclsForOtherDomain(Domain.LIMITED))
                    .withKafkaAcls(aclsForOtherDomain(Domain.UNRELATED))
                    .build();

    @Test
    @Order(1)
    void shouldProvisionExistingSpec() {
        try (Admin adminClient = KAFKA_ENV.adminClient()) {
            AclProvisioner.provision(false, false, API_SPEC, adminClient);
        }
    }

    @Test
    @Order(2)
    void shouldPublishUpdatedAcls() {
        try (Admin adminClient = KAFKA_ENV.adminClient()) {
            final var dryRunAcls =
                    AclProvisioner.provision(true, false, API_UPDATE_SPEC, adminClient);
            assertThat(dryRunAcls, is(hasSize(2)));
            assertThat(
                    dryRunAcls.stream().filter(acl -> acl.state().equals(STATE.CREATE)).count(),
                    is(2L));

            final var createdAcls =
                    AclProvisioner.provision(false, false, API_UPDATE_SPEC, adminClient);

            assertThat(createdAcls, is(hasSize(2)));
            assertThat(
                    createdAcls.stream().filter(acl -> acl.state().equals(STATE.CREATED)).count(),
                    is(2L));
        }
    }

    @Test
    @Order(3)
    void shouldCleanUnSpecdAcls()
            throws ExecutionException, InterruptedException, TimeoutException {
        try (Admin adminClient = KAFKA_ENV.adminClient()) {

            adminClient.deleteAcls(List.of(AclBindingFilter.ANY));

            // Setup UnExpected ACL
            final var pattern =
                    new ResourcePattern(
                            ResourceType.TOPIC,
                            API_SPEC.id() + "_public.something.NOT_NEEDED",
                            PatternType.LITERAL);
            final var aclBinding1 =
                    new AclBinding(pattern, API_SPEC.requiredAcls().iterator().next().entry());

            adminClient
                    .createAcls(List.of(aclBinding1))
                    .all()
                    .get(Provisioner.REQUEST_TIMEOUT, TimeUnit.SECONDS);

            final var dryRunAcls = AclProvisioner.provision(true, true, API_SPEC, adminClient);
            assertThat(dryRunAcls, is(hasSize(1)));
            assertThat(dryRunAcls.iterator().next().state(), is(STATE.DELETE));

            final var createdAcls = AclProvisioner.provision(false, true, API_SPEC, adminClient);

            assertThat(createdAcls, is(hasSize(1)));
            assertThat(createdAcls.iterator().next().state(), is(STATE.DELETED));
        }
    }

    private static Set<AclBinding> aclsForOtherDomain(final Domain domain) {
        final String principal = "User:" + domain.domainId;
        return Set.of(
                new AclBinding(
                        new ResourcePattern(CLUSTER, CLUSTER_NAME, LITERAL),
                        new AccessControlEntry(principal, "*", IDEMPOTENT_WRITE, ALLOW)),
                new AclBinding(
                        new ResourcePattern(GROUP, domain.domainId, LITERAL),
                        new AccessControlEntry(principal, "*", ALL, ALLOW)),
                new AclBinding(
                        new ResourcePattern(TRANSACTIONAL_ID, domain.domainId, PREFIXED),
                        new AccessControlEntry(principal, "*", ALL, ALLOW)));
    }
}
