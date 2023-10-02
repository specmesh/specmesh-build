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

package io.specmesh.kafka;

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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.specmesh.kafka.provision.AclProvisioner;
import io.specmesh.kafka.provision.Status.STATE;
import io.specmesh.kafka.provision.TopicProvisioner;
import io.specmesh.kafka.provision.TopicProvisioner.Topic;
import io.specmesh.test.TestSpecLoader;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.resource.ResourcePattern;
import org.hamcrest.Matchers;
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
class ProvisionerUpdatingFunctionalTest {

    private static final KafkaApiSpec API_SPEC =
            TestSpecLoader.loadFromClassPath("provisioner-functional-test-api.yaml");

    private static final KafkaApiSpec API_UPDATE_SPEC =
            TestSpecLoader.loadFromClassPath("provisioner-update-functional-test-api.yaml");

    public static final String USER_SIGNED_UP = "simple.provision_demo._public.user_signed_up";

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
            TopicProvisioner.provision(false, false, API_SPEC, adminClient);
            AclProvisioner.provision(false, API_SPEC, adminClient);
        }
    }

    @Test
    @Order(2)
    void shouldDoTopicUpdates() {
        try (Admin adminClient = KAFKA_ENV.adminClient()) {

            // DRY RUN Test
            final var dryRunChangeset =
                    TopicProvisioner.provision(true, false, API_UPDATE_SPEC, adminClient);

            assertThat(
                    dryRunChangeset.stream().map(Topic::name).collect(Collectors.toSet()),
                    Matchers.is(Matchers.hasItem(USER_SIGNED_UP)));

            final var dryFirstUpdate = dryRunChangeset.iterator().next();

            assertThat(dryFirstUpdate.state(), Matchers.is(STATE.UPDATE));

            // REAL Test
            final var changeset =
                    TopicProvisioner.provision(false, false, API_UPDATE_SPEC, adminClient);

            final var change = changeset.iterator().next();

            assertThat(change.name(), Matchers.is(USER_SIGNED_UP));
            assertThat(change.partitions(), Matchers.is(99));
            assertThat(change.messages(), Matchers.is(Matchers.containsString("partitions")));
            assertThat(change.config().get(TopicConfig.RETENTION_MS_CONFIG), Matchers.is("999000"));
            assertThat(
                    change.messages(),
                    Matchers.is(Matchers.containsString(TopicConfig.RETENTION_MS_CONFIG)));
        }
    }

    @Test
    @Order(4)
    void shouldPublishUpdatedAcls() {
        try (Admin adminClient = KAFKA_ENV.adminClient()) {
            final var dryRunAcls = AclProvisioner.provision(true, API_UPDATE_SPEC, adminClient);
            assertThat(dryRunAcls, Matchers.is(Matchers.hasSize(2)));
            assertThat(
                    dryRunAcls.stream().filter(acl -> acl.state().equals(STATE.CREATE)).count(),
                    Matchers.is(2L));

            final var createdAcls = AclProvisioner.provision(false, API_UPDATE_SPEC, adminClient);

            assertThat(createdAcls, Matchers.is(Matchers.hasSize(2)));
            assertThat(
                    createdAcls.stream().filter(acl -> acl.state().equals(STATE.CREATED)).count(),
                    Matchers.is(2L));
        }
    }

    @Test
    @Order(5)
    void shouldCleanupNonSpecTopicsDryRun()
            throws ExecutionException, InterruptedException, TimeoutException {
        try (Admin adminClient = KAFKA_ENV.adminClient()) {
            adminClient
                    .createTopics(
                            List.of(new NewTopic(API_SPEC.id() + ".should.not.be", 1, (short) 1)))
                    .all()
                    .get(20, TimeUnit.SECONDS);

            assertThat(topicCount(adminClient), Matchers.is(3L));

            // create the unspecified topic
            final var unSpecifiedTopics =
                    TopicProvisioner.provision(true, true, API_SPEC, adminClient);
            // 'should.not.be' topic that should not be
            assertThat(unSpecifiedTopics, Matchers.is(Matchers.hasSize(1)));
            assertThat(topicCount(adminClient), Matchers.is(3L));
        }
    }

    @Test
    @Order(6)
    void shouldCleanupNonSpecTopicsIRL() throws ExecutionException, InterruptedException {
        try (Admin adminClient = KAFKA_ENV.adminClient()) {

            assertThat(topicCount(adminClient), Matchers.is(3L));

            // create the unspecified topic
            final var unSpecifiedTopics =
                    TopicProvisioner.provision(false, true, API_SPEC, adminClient);

            // 'should.not.be' topic that should not be
            assertThat(unSpecifiedTopics, Matchers.is(Matchers.hasSize(1)));

            // 'should.not.be' topic was removed
            assertThat(topicCount(adminClient), Matchers.is(2L));
        }
    }

    private static long topicCount(final Admin adminClient)
            throws InterruptedException, ExecutionException {
        return adminClient.listTopics().listings().get().stream()
                .filter(t -> t.name().startsWith(API_SPEC.id()))
                .count();
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
