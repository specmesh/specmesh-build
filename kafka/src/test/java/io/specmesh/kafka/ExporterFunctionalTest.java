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
import static org.hamcrest.Matchers.is;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.specmesh.apiparser.model.ApiSpec;
import io.specmesh.apiparser.model.Bindings;
import io.specmesh.apiparser.model.Channel;
import io.specmesh.apiparser.model.KafkaBinding;
import io.specmesh.kafka.provision.Provisioner;
import io.specmesh.test.TestSpecLoader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.resource.ResourcePattern;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@SuppressFBWarnings(
        value = "IC_INIT_CIRCULARITY",
        justification = "shouldHaveInitializedEnumsCorrectly() proves this is false positive")
class ExporterFunctionalTest {

    private static final String aggregateId = ".london.hammersmith.olympia.bigdatalondon";

    private static final KafkaApiSpec API_SPEC =
            TestSpecLoader.loadFromClassPath("apispec-functional-test-app.yaml");

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
                    .withoutSchemaRegistry()
                    .withSaslAuthentication(
                            ADMIN_USER,
                            ADMIN_USER + "-secret",
                            Domain.SELF.domainId,
                            Domain.SELF.domainId + "-secret",
                            Domain.UNRELATED.domainId,
                            Domain.UNRELATED.domainId + "-secret",
                            Domain.LIMITED.domainId,
                            Domain.LIMITED.domainId + "-secret")
                    .withKafkaAcls(aclsForOtherDomain(Domain.LIMITED))
                    .withKafkaAcls(aclsForOtherDomain(Domain.UNRELATED))
                    .build();

    @BeforeAll
    static void setUp() {
        Provisioner.builder()
                .apiSpec(API_SPEC)
                .schemaPath("./build/resources/test")
                .adminClient(KAFKA_ENV.adminClient())
                .closeAdminClient(true)
                .srDisabled(true)
                .build()
                .provision()
                .check();
    }

    @Test
    void shouldExportAPIFromCluster() throws Exporter.ExporterException {

        try (Admin adminClient = KAFKA_ENV.adminClient()) {
            final ApiSpec exported = Exporter.export(aggregateId, adminClient);
            assertThat(
                    exported.channels().keySet(),
                    containsInAnyOrder(
                            ".london.hammersmith.olympia.bigdatalondon._protected.retail.subway.food.purchase",
                            ".london.hammersmith.olympia.bigdatalondon._public.attendee",
                            ".london.hammersmith.olympia.bigdatalondon._private.retail.subway.customers"));
        }
    }

    @Test
    void shouldExportYAMLAPIFromCluster() throws Exporter.ExporterException, IOException {

        final ApiSpec apiSpec =
                ApiSpec.builder()
                        .id("urn:asyncapi-id")
                        .version("version-123")
                        .channels(
                                Map.of(
                                        "one-topic-channel",
                                        Channel.builder()
                                                .description("one-topic-channel-description")
                                                .bindings(
                                                        Bindings.builder()
                                                                .kafka(
                                                                        KafkaBinding.builder()
                                                                                .groupId(
                                                                                        "kafka-binding-group-id")
                                                                                .partitions(
                                                                                        Optional.of(
                                                                                                1))
                                                                                .replicas(
                                                                                        Optional.of(
                                                                                                (short)
                                                                                                        3))
                                                                                .build())
                                                                .build())
                                                .build()))
                        .build();
        assertThat(
                new ExporterYamlWriter().export(apiSpec),
                is(
                        new String(
                                ExporterFunctionalTest.class
                                        .getClassLoader()
                                        .getResourceAsStream("exporter-expected-spec.yaml")
                                        .readAllBytes(),
                                StandardCharsets.UTF_8)));
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
