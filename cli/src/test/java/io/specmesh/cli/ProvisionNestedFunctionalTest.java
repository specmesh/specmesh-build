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

package io.specmesh.cli;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import io.specmesh.kafka.DockerKafkaEnvironment;
import io.specmesh.kafka.KafkaEnvironment;
import io.specmesh.kafka.provision.TopicProvisioner.Topic;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import picocli.CommandLine;

class ProvisionNestedFunctionalTest {

    private static final String OWNER_USER = "simple.schema_demo";

    @RegisterExtension
    private static final KafkaEnvironment KAFKA_ENV =
            DockerKafkaEnvironment.builder()
                    .withSaslAuthentication(
                            "admin", "admin-secret", OWNER_USER, OWNER_USER + "-secret")
                    .withKafkaAcls()
                    .build();

    private Admin admin;

    @BeforeEach
    void setUp() {
        admin = KAFKA_ENV.adminClient();
    }

    @AfterEach
    void tearDown() {
        admin.close();
    }

    /**
     * Provision all resources apart from schemas. note: Dont specify SR credentials to avoid schema
     * publishing
     *
     * @throws Exception - when broken
     */
    @Test
    void shouldProvisionTopicsAndAclResourcesWithNestedSchemasAndRepublishCorrectly()
            throws Exception {
        // Given:
        final Provision provision = new Provision();
        final CommandLine.ParseResult parseResult =
                new CommandLine(provision)
                        .parseArgs(
                                ("--bootstrap-server "
                                                + KAFKA_ENV.kafkaBootstrapServers()
                                                + " --spec nested_schema_demo-api.yaml"
                                                + " --username admin"
                                                + " --secret admin-secret"
                                                + " --schema-registry "
                                                + KAFKA_ENV.schemaRegistryServer()
                                                + " --schema-path ./resources"
                                                + " -Dsome.property=testOne"
                                                + " -Dsome.other.property=testTwo")
                                        .split(" "));

        assertThat(parseResult.matchedArgs().size(), is(8));

        // check system properties
        assertThat(System.getProperty("some.property"), is("testOne"));
        assertThat(System.getProperty("some.other.property"), is("testTwo"));

        // When:
        final var status = provision.run();

        final var statusRepublish = provision.run();

        // then: Verify status is correct
        assertThat(status.failed(), is(false));
        assertThat(statusRepublish.failed(), is(false));
        final var topicProvisionStatus = status.topics();
        assertThat(
                topicProvisionStatus.stream().map(Topic::name).collect(Collectors.toSet()),
                is(
                        containsInAnyOrder(
                                "simple.schema_demo._public.user_signed_up",
                                "simple.schema_demo._public.user_signed_up1",
                                "simple.schema_demo._public.user_checkout",
                                "simple.schema_demo._public.user_info")));

        /*
         * Loose sanity checks follow because we don't need to retest the tests that other tests test
         */

        // Then: check topic resources were created
        final ListTopicsResult topicsResult = admin.listTopics();
        final Set<String> topicNames = topicsResult.names().get();
        assertThat(
                topicNames,
                is(
                        containsInAnyOrder(
                                "simple.schema_demo._public.user_signed_up",
                                "simple.schema_demo._public.user_signed_up1",
                                "simple.schema_demo._public.user_checkout",
                                "simple.schema_demo._public.user_info",
                                "_schemas")));

        assertThat(cleanupPolicy("simple.schema_demo._public.user_signed_up1"), is("delete"));
        assertThat(cleanupPolicy("simple.schema_demo._public.user_checkout"), is("compact,delete"));

        assertThat(status.schemas(), hasSize(4));
    }

    private String cleanupPolicy(final String topicName) {
        try {
            final ConfigResource checkoutTopic =
                    new ConfigResource(ConfigResource.Type.TOPIC, topicName);
            final Config config =
                    admin.describeConfigs(List.of(checkoutTopic))
                            .values()
                            .get(checkoutTopic)
                            .get(30, TimeUnit.SECONDS);

            return config.get("cleanup.policy").value();
        } catch (Exception e) {
            throw new AssertionError("failed to get topic cleanup.policy for: " + topicName, e);
        }
    }
}
