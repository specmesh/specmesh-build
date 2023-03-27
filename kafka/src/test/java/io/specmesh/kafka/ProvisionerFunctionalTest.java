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
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.specmesh.test.TestSpecLoader;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.resource.ResourcePattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@SuppressFBWarnings(
        value = "IC_INIT_CIRCULARITY",
        justification = "shouldHaveInitializedEnumsCorrectly() proves this is false positive")
class ProvisionerFunctionalTest {

    private static final KafkaApiSpec API_SPEC =
            TestSpecLoader.loadFromClassPath("provisioner-functional-test-api.yaml");

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
    void shouldProvisionFromZero() {
        try (Admin adminClient = KAFKA_ENV.adminClient()) {
            final var schemaRegistryClient =
                    new CachedSchemaRegistryClient(KAFKA_ENV.schemeRegistryServer(), 5);
            final var validateStatus =
                    Provisioner.provision(
                            true,
                            API_SPEC,
                            "./build/resources/test",
                            adminClient,
                            Optional.of(schemaRegistryClient));
            final var provisionStatus =
                    Provisioner.provision(
                            false,
                            API_SPEC,
                            "./build/resources/test",
                            adminClient,
                            Optional.of(schemaRegistryClient));

            validateStatus.build();
            provisionStatus.build();

            // Verify -- topics --  'validateMode' - only 'CREATE's
            final var validateTopicStatusMap = validateStatus.topics().status();
            assertThat(validateTopicStatusMap.size(), is(2));
            assertThat(
                    validateTopicStatusMap.keySet(),
                    is(
                            containsInAnyOrder(
                                    "simple.schema_demo._public.user_signed_up",
                                    "simple.schema_demo._public.user_checkout")));
            assertThat(
                    validateTopicStatusMap.values().stream()
                            .filter(tStatus -> tStatus.crud() == ProvisionStatus.CRUD.CREATE)
                            .count(),
                    is(2L));

            // Verify -- topics
            final var topicStatusMap = provisionStatus.topics().status();
            assertThat(topicStatusMap.size(), is(2));
            assertThat(
                    topicStatusMap.keySet(),
                    is(
                            containsInAnyOrder(
                                    "simple.schema_demo._public.user_signed_up",
                                    "simple.schema_demo._public.user_checkout")));
            assertThat(
                    topicStatusMap.values().stream()
                            .filter(tStatus -> tStatus.crud() == ProvisionStatus.CRUD.CREATED)
                            .count(),
                    is(2L));

            // Verify -- ACLs
            final var validateAclStatusMap = validateStatus.acls().status();
            final var aclStatusMap = provisionStatus.acls().status();

            assertThat(validateAclStatusMap.size(), is(10));

            assertThat(
                    validateAclStatusMap.values().stream()
                            .filter(tStatus -> tStatus.crud() == ProvisionStatus.CRUD.CREATE)
                            .count(),
                    is(10L));

            assertThat(
                    aclStatusMap.values().stream()
                            .filter(tStatus -> tStatus.crud() == ProvisionStatus.CRUD.CREATED)
                            .count(),
                    is(10L));

            // Verify -- Schemas
            final var validateSchemaStatusMap = validateStatus.schemas().status();
            final var schemaStatusMap = provisionStatus.schemas().status();

            assertThat(validateSchemaStatusMap.size(), is(2));

            assertThat(
                    validateSchemaStatusMap.values().stream()
                            .filter(tStatus -> tStatus.crud() == ProvisionStatus.CRUD.CREATE)
                            .count(),
                    is(2L));
            assertThat(
                    validateSchemaStatusMap.keySet(),
                    is(
                            containsInAnyOrder(
                                    "simple.schema_demo._public.user_signed_up-value",
                                    "simple.schema_demo._public.user_checkout-value")));

            assertThat(
                    schemaStatusMap.values().stream()
                            .filter(tStatus -> tStatus.crud() == ProvisionStatus.CRUD.CREATED)
                            .count(),
                    is(2L));
            assertThat(
                    schemaStatusMap.keySet(),
                    is(
                            containsInAnyOrder(
                                    "simple.schema_demo._public.user_signed_up-value",
                                    "simple.schema_demo._public.user_checkout-value")));
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
