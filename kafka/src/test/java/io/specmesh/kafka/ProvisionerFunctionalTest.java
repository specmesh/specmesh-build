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
import io.specmesh.kafka.provision.ProvisionTopics;
import io.specmesh.kafka.provision.Status;
import io.specmesh.test.TestSpecLoader;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.resource.ResourcePattern;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

@SuppressFBWarnings(
        value = "IC_INIT_CIRCULARITY",
        justification = "shouldHaveInitializedEnumsCorrectly() proves this is false positive")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ProvisionerFunctionalTest {

    private static final KafkaApiSpec API_SPEC =
            TestSpecLoader.loadFromClassPath("provisioner-functional-test-api.yaml");
    public static final String USER_SIGNED_UP = "simple.provision_demo._public.user_signed_up";
    public static final String USER_CHECKOUT = "simple.provision_demo._public.user_checkout";

    private enum Domain {
        /** The domain associated with the spec. */
        SELF(API_SPEC.id()),
        /** An unrelated domain. */
        UNRELATED(".london.hammersmith.transport"),
        /** A domain granted access to the protected topic. */
        LIMITED(".some.other.domain.root");

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
    void shouldDryRunTopicsFromEmptyCluster() {
        try (Admin adminClient = KAFKA_ENV.adminClient()) {

            final var changeset = ProvisionTopics.provision(true, API_SPEC, adminClient);

            assertThat(
                    changeset.stream().map(ProvisionTopics.Topic::name).collect(Collectors.toSet()),
                    Matchers.is(Matchers.containsInAnyOrder(USER_SIGNED_UP, USER_CHECKOUT)));

            assertThat(
                    "dry run should leave changeset in 'create' state",
                    changeset.stream()
                            .filter(topic -> topic.state() == Status.STATE.CREATE)
                            .count(),
                    Matchers.is(2L));

            assertThat(
                    changeset.stream()
                            .filter(topic -> topic.name().equals(USER_SIGNED_UP))
                            .findFirst()
                            .get()
                            .partitions(),
                    Matchers.is(10));

            assertThat(
                    changeset.stream()
                            .filter(topic -> topic.name().equals(USER_SIGNED_UP))
                            .findFirst()
                            .get()
                            .config()
                            .get(TopicConfig.RETENTION_MS_CONFIG),
                    Matchers.is("3600000"));

            assertThat(
                    changeset.stream()
                            .filter(topic -> topic.name().equals(USER_CHECKOUT))
                            .findFirst()
                            .get()
                            .config()
                            .get(TopicConfig.CLEANUP_POLICY_CONFIG),
                    Matchers.is(TopicConfig.CLEANUP_POLICY_DELETE));
        }
    }

    @Test
    @Order(2)
    void shouldProvisionTopicsFromEmptyCluster() throws ExecutionException, InterruptedException {
        try (Admin adminClient = KAFKA_ENV.adminClient()) {

            final var changeSet = ProvisionTopics.provision(false, API_SPEC, adminClient);

            // Verify - changeset
            assertThat(
                    changeSet.stream().map(ProvisionTopics.Topic::name).collect(Collectors.toSet()),
                    Matchers.is(Matchers.containsInAnyOrder(USER_SIGNED_UP, USER_CHECKOUT)));

            assertThat(
                    "dry run should leave changeset in 'create' state",
                    changeSet.stream()
                            .filter(topic -> topic.state() == Status.STATE.CREATED)
                            .count(),
                    Matchers.is(2L));

            assertThat(
                    changeSet.stream()
                            .filter(topic -> topic.name().equals(USER_SIGNED_UP))
                            .findFirst()
                            .get()
                            .partitions(),
                    Matchers.is(10));

            assertThat(
                    changeSet.stream()
                            .filter(topic -> topic.name().equals(USER_SIGNED_UP))
                            .findFirst()
                            .get()
                            .config()
                            .get(TopicConfig.RETENTION_MS_CONFIG),
                    Matchers.is("3600000"));

            assertThat(
                    changeSet.stream()
                            .filter(topic -> topic.name().equals(USER_CHECKOUT))
                            .findFirst()
                            .get()
                            .config()
                            .get(TopicConfig.CLEANUP_POLICY_CONFIG),
                    Matchers.is(TopicConfig.CLEANUP_POLICY_DELETE));

            // Verify cluster version of the topic matches
            final var topicListings =
                    adminClient.listTopics().listings().get().stream()
                            .filter(topic -> topic.name().startsWith("simple"))
                            .collect(Collectors.toList());
            assertThat(topicListings, Matchers.is(Matchers.hasSize(2)));

            // Verify description - check partitions and replicas
            final var topicDescriptions =
                    adminClient
                            .describeTopics(
                                    TopicCollection.ofTopicNames(
                                            List.of(USER_SIGNED_UP, USER_CHECKOUT)))
                            .topicNameValues();
            final var userSignedUpDes = topicDescriptions.get(USER_SIGNED_UP).get();
            assertThat(userSignedUpDes.partitions(), Matchers.is(Matchers.hasSize(10)));
            assertThat(
                    userSignedUpDes.partitions().get(0).replicas(),
                    Matchers.is(Matchers.hasSize(1)));

            // Verify config - check retention & cleanup
            final var configResource =
                    new ConfigResource(ConfigResource.Type.TOPIC, USER_SIGNED_UP);
            final var configs = adminClient.describeConfigs(List.of(configResource)).all().get();

            final var topicConfig = configs.get(configResource);
            assertThat(
                    topicConfig.get(TopicConfig.CLEANUP_POLICY_CONFIG).value(),
                    Matchers.is(TopicConfig.CLEANUP_POLICY_DELETE));
            assertThat(
                    topicConfig.get(TopicConfig.RETENTION_MS_CONFIG).value(),
                    Matchers.is("3600000"));
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
