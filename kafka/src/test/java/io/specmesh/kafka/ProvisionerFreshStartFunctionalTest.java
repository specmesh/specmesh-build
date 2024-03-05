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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.specmesh.kafka.provision.AclProvisioner;
import io.specmesh.kafka.provision.Status;
import io.specmesh.kafka.provision.TopicProvisioner;
import io.specmesh.kafka.provision.TopicProvisioner.Topic;
import io.specmesh.kafka.provision.schema.SchemaProvisioner;
import io.specmesh.test.TestSpecLoader;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.resource.ResourcePattern;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Tests execution DryRuns and UPDATES where the provisioner-functional-test-api.yml is NOT
 * provisioned
 */
@SuppressFBWarnings(
        value = "IC_INIT_CIRCULARITY",
        justification = "shouldHaveInitializedEnumsCorrectly() proves this is false positive")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ProvisionerFreshStartFunctionalTest {

    private static final KafkaApiSpec API_SPEC =
            TestSpecLoader.loadFromClassPath("provisioner-functional-test-api.yaml");
    public static final String USER_SIGNED_UP = "simple.provision_demo._public.user_signed_up";
    public static final String USER_INFO = "simple.provision_demo._protected.user_info";

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
    void shouldDryRunTopicsFromEmptyCluster() {
        try (Admin adminClient = KAFKA_ENV.adminClient()) {

            final var changeset = TopicProvisioner.provision(true, false, API_SPEC, adminClient);

            assertThat(
                    changeset.stream().map(Topic::name).collect(Collectors.toSet()),
                    is(containsInAnyOrder(USER_SIGNED_UP, USER_INFO)));

            assertThat(
                    "dry run should leave changeset in 'create' state",
                    changeset.stream()
                            .filter(topic -> topic.state() == Status.STATE.CREATE)
                            .count(),
                    is(2L));

            assertThat(
                    changeset.stream()
                            .filter(topic -> topic.name().equals(USER_SIGNED_UP))
                            .findFirst()
                            .get()
                            .partitions(),
                    is(10));

            assertThat(
                    changeset.stream()
                            .filter(topic -> topic.name().equals(USER_SIGNED_UP))
                            .findFirst()
                            .get()
                            .config()
                            .get(TopicConfig.RETENTION_MS_CONFIG),
                    is("3600000"));

            assertThat(
                    changeset.stream()
                            .filter(topic -> topic.name().equals(USER_INFO))
                            .findFirst()
                            .get()
                            .config()
                            .get(TopicConfig.CLEANUP_POLICY_CONFIG),
                    is(TopicConfig.CLEANUP_POLICY_DELETE));
        }
    }

    @Test
    @Order(2)
    void shouldDryRunACLsFromEmptyCluster() {
        try (Admin adminClient = KAFKA_ENV.adminClient()) {

            final var changeset = AclProvisioner.provision(true, false, API_SPEC, adminClient);

            // Verify - 11 created
            assertThat(
                    "dry run should leave changeset in 'create' state",
                    changeset.stream()
                            .filter(topic -> topic.state() == Status.STATE.CREATE)
                            .count(),
                    is(12L));

            // Verify - should have 6 TOPIC, 2 GROUP, 2 TRANSACTIONAL and 1 CLUSTER Acls
            assertThat(
                    changeset.stream().filter(aacl -> aacl.name().contains("TOPIC")).count(),
                    is(8L));

            assertThat(
                    changeset.stream()
                            .filter(aacl -> aacl.name().contains("TRANSACTIONAL_ID"))
                            .count(),
                    is(2L));

            assertThat(
                    changeset.stream().filter(aacl -> aacl.name().contains("GROUP")).count(),
                    is(1L));

            assertThat(
                    changeset.stream().filter(aacl -> aacl.name().contains("CLUSTER")).count(),
                    is(1L));
        }
    }

    @Test
    @Order(3)
    void shouldDryRunSchemasFromEmptyCluster() throws RestClientException, IOException {

        final var srClient = KAFKA_ENV.srClient();
        final var changeset =
                SchemaProvisioner.provision(
                        true, false, API_SPEC, "./build/resources/test", srClient);

        // Verify - 11 created
        assertThat(
                "dry run should leave changeset in 'create' state",
                changeset.stream().filter(topic -> topic.state() == Status.STATE.CREATE).count(),
                is(2L));

        // Verify - should have 2 SR entries
        final var allSubjects = srClient.getAllSubjects();

        assertThat(allSubjects, is(hasSize(0)));
    }

    @Test
    @Order(4)
    void shouldProvisionTopicsFromEmptyCluster() throws ExecutionException, InterruptedException {
        try (Admin adminClient = KAFKA_ENV.adminClient()) {

            final var changeSet = TopicProvisioner.provision(false, false, API_SPEC, adminClient);

            // Verify - changeset
            assertThat(
                    changeSet.stream().map(Topic::name).collect(Collectors.toSet()),
                    is(containsInAnyOrder(USER_SIGNED_UP, USER_INFO)));

            assertThat(
                    "changeset should only have 2 updates",
                    changeSet.stream()
                            .filter(topic -> topic.state() == Status.STATE.CREATED)
                            .count(),
                    is(2L));

            assertThat(
                    changeSet.stream()
                            .filter(topic -> topic.name().equals(USER_SIGNED_UP))
                            .findFirst()
                            .get()
                            .partitions(),
                    is(10));

            assertThat(
                    changeSet.stream()
                            .filter(topic -> topic.name().equals(USER_SIGNED_UP))
                            .findFirst()
                            .get()
                            .config()
                            .get(TopicConfig.RETENTION_MS_CONFIG),
                    is("3600000"));

            assertThat(
                    changeSet.stream()
                            .filter(topic -> topic.name().equals(USER_INFO))
                            .findFirst()
                            .get()
                            .config()
                            .get(TopicConfig.CLEANUP_POLICY_CONFIG),
                    is(TopicConfig.CLEANUP_POLICY_DELETE));

            // Verify cluster version of the topic matches
            final var topicListings =
                    adminClient.listTopics().listings().get().stream()
                            .filter(topic -> topic.name().startsWith("simple"))
                            .collect(Collectors.toList());
            assertThat(topicListings, is(hasSize(2)));

            // Verify description - check partitions and replicas
            final var topicDescriptions =
                    adminClient
                            .describeTopics(
                                    TopicCollection.ofTopicNames(
                                            List.of(USER_SIGNED_UP, USER_INFO)))
                            .topicNameValues();
            final var userSignedUpDes = topicDescriptions.get(USER_SIGNED_UP).get();
            assertThat(userSignedUpDes.partitions(), is(hasSize(10)));
            assertThat(userSignedUpDes.partitions().get(0).replicas(), is(hasSize(1)));

            // Verify config - check retention & cleanup
            final var configResource =
                    new ConfigResource(ConfigResource.Type.TOPIC, USER_SIGNED_UP);
            final var configs = adminClient.describeConfigs(List.of(configResource)).all().get();

            final var topicConfig = configs.get(configResource);
            assertThat(
                    topicConfig.get(TopicConfig.CLEANUP_POLICY_CONFIG).value(),
                    is(TopicConfig.CLEANUP_POLICY_DELETE));
            assertThat(topicConfig.get(TopicConfig.RETENTION_MS_CONFIG).value(), is("3600000"));
        }
    }

    @Test
    @Order(5)
    void shouldDoRealACLsFromEmptyCluster() throws ExecutionException, InterruptedException {
        try (Admin adminClient = KAFKA_ENV.adminClient()) {

            final var changeset = AclProvisioner.provision(false, false, API_SPEC, adminClient);

            // Verify - 11 created
            assertThat(
                    "dry run should leave changeset in 'create' state",
                    changeset.stream()
                            .filter(topic -> topic.state() == Status.STATE.CREATED)
                            .count(),
                    is(12L));

            final var bindings = adminClient.describeAcls(AclBindingFilter.ANY).values().get();

            final var provisionDemoBindings =
                    bindings.stream()
                            .filter(binding -> binding.toString().contains("provision_demo"))
                            .collect(Collectors.toList());

            // Verfiy 'provision_demo' has 6 topics, 2 transqction and 1 group

            assertThat(
                    provisionDemoBindings.stream()
                            .filter(binding -> binding.toString().contains("TOPIC"))
                            .count(),
                    is(8L));
            assertThat(
                    provisionDemoBindings.stream()
                            .filter(binding -> binding.toString().contains("TRANSACTIONAL_ID"))
                            .count(),
                    is(2L));
            assertThat(
                    provisionDemoBindings.stream()
                            .filter(binding -> binding.toString().contains("GROUP"))
                            .count(),
                    is(1L));

            assertThat(
                    provisionDemoBindings.stream()
                            .filter(binding -> binding.toString().contains("CLUSTER"))
                            .count(),
                    is(1L));
        }
    }

    @Test
    @Order(6)
    void shouldPublishSchemasFromEmptyCluster() throws RestClientException, IOException {

        final var srClient = KAFKA_ENV.srClient();
        final var changeset =
                SchemaProvisioner.provision(
                        false, false, API_SPEC, "./build/resources/test", srClient);

        // Verify - 11 created
        assertThat(
                changeset.stream().filter(topic -> topic.state() == Status.STATE.CREATED).count(),
                is(2L));

        // Verify - should have 2 SR entries
        final var allSubjects = srClient.getAllSubjects();

        assertThat(allSubjects, is(hasSize(2)));

        final var schemas = srClient.getSchemas("simple", false, false);

        final var schemaNames =
                schemas.stream().map(ParsedSchema::name).collect(Collectors.toSet());

        assertThat(
                schemaNames,
                is(
                        containsInAnyOrder(
                                "io.specmesh.kafka.schema.UserInfo",
                                "simple.provision_demo._public.user_signed_up_value.UserSignedUp")));
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
