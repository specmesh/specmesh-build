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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import io.specmesh.kafka.DockerKafkaEnvironment;
import io.specmesh.kafka.KafkaEnvironment;
import io.specmesh.kafka.provision.Status;
import io.specmesh.kafka.provision.TopicProvisioner.Topic;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import picocli.CommandLine;

class ProvisionFunctionalTest {

    private static final String OWNER_USER = "simple.schema_demo";

    @RegisterExtension
    private static final KafkaEnvironment KAFKA_ENV =
            DockerKafkaEnvironment.builder()
                    .withSaslAuthentication(
                            "admin", "admin-secret", OWNER_USER, OWNER_USER + "-secret")
                    .withKafkaAcls()
                    .build();

    @Test
    void shouldProvisionCmdWithLombock() throws Exception {
        final var provision = Provision.builder().brokerUrl("borker").aclDisabled(false).build();
        assertThat(provision.aclDisabled(), is(false));
        assertThat(provision.brokerUrl(), is("borker"));
    }

    @Test
    void shouldNotSwallowExceptions() throws Exception {
        final var topic = Topic.builder().build();
        final var swallowed =
                Status.builder()
                        .topics(
                                List.of(
                                        topic.exception(
                                                new RuntimeException(
                                                        new RuntimeException("swallowed")))))
                        .build();
        assertThat(
                swallowed.topics().iterator().next().exception().toString(),
                containsString(".java:"));
    }

    /**
     * Provision all resources apart from schemas. note: Dont specify SR credentials to avoid schema
     * publishing
     *
     * @throws Exception - when broken
     */
    @Test
    void shouldProvisionTopicsAndAclResourcesWithSchemas() throws Exception {

        final Provision provision = Provision.builder().build();

        // Given:
        final CommandLine.ParseResult parseResult =
                new CommandLine(provision)
                        .parseArgs(
                                ("--bootstrap-server "
                                                + KAFKA_ENV.kafkaBootstrapServers()
                                                + " --spec simple_schema_demo-api.yaml"
                                                + " --username admin"
                                                + " --secret admin-secret"
                                                + " --schema-registry "
                                                + KAFKA_ENV.schemeRegistryServer()
                                                + " --schema-path ./resources"
                                                + " -Dsome.property=testOne"
                                                + " -Dsome.other.property=testTwo")
                                        .split(" "));

        assertThat(parseResult.matchedArgs().size(), is(8));

        assertThat(provision.call(), is(0));

        // check system properties
        assertThat(System.getProperty("some.property"), is("testOne"));
        assertThat(System.getProperty("some.other.property"), is("testTwo"));

        // When:
        final var status = provision.state();

        // then: Verify status is correct
        final var topicProvisionStatus = status.topics();
        assertThat(
                topicProvisionStatus.stream().map(Topic::name).collect(Collectors.toSet()),
                is(
                        containsInAnyOrder(
                                "simple.schema_demo._public.user_signed_up",
                                "simple.schema_demo._public.user_checkout",
                                "simple.schema_demo._public.user_info")));

        /*
         * Loose sanity checks follow because we don't need to retest the tests that other tests test
         */

        // Then: check topic resources were created
        final ListTopicsResult topicsResult = KAFKA_ENV.adminClient().listTopics();
        final Set<String> topicNames = topicsResult.names().get();
        assertThat(
                topicNames,
                is(
                        containsInAnyOrder(
                                "simple.schema_demo._public.user_signed_up",
                                "simple.schema_demo._public.user_checkout",
                                "simple.schema_demo._public.user_info",
                                "_schemas")));

        assertThat(status.schemas(), hasSize(3));
    }
}
