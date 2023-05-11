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
import static org.hamcrest.Matchers.is;

import io.specmesh.kafka.DockerKafkaEnvironment;
import io.specmesh.kafka.KafkaEnvironment;
import io.specmesh.kafka.provision.TopicProvisioner.Topic;
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

    /**
     * Provision all resources apart from schemas. note: Dont specify SR credentials to avoid schema
     * publishing
     *
     * @throws Exception - when broken
     */
    @Test
    void shouldProvisionTopicsAndAclResourcesWithoutSchemas() throws Exception {

        final Provision provision = new Provision();

        // Given:
        final CommandLine.ParseResult parseResult =
                new CommandLine(provision)
                        .parseArgs(
                                ("--bootstrap-server "
                                                + KAFKA_ENV.kafkaBootstrapServers()
                                                + " --spec simple_spec_demo-api.yaml"
                                                + " --username admin"
                                                + " --secret admin-secret"
                                                + " --schema-registry "
                                                + KAFKA_ENV.schemeRegistryServer()
                                                + " --schema-path ./resources")
                                        .split(" "));

        assertThat(parseResult.matchedArgs().size(), is(6));

        assertThat(provision.call(), is(0));

        // When:
        final var status = provision.state();

        // then: Verify status is correct
        final var topicProvisionStatus = status.topics();
        assertThat(
                topicProvisionStatus.stream().map(Topic::name).collect(Collectors.toSet()),
                is(
                        containsInAnyOrder(
                                "simple.spec_demo._public.user_signed_up",
                                "simple.spec_demo._private.user_checkout",
                                "simple.spec_demo._protected.purchased")));

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
                                "simple.spec_demo._public.user_signed_up",
                                "simple.spec_demo._private.user_checkout",
                                "simple.spec_demo._protected.purchased",
                                "_schemas")));
    }
}
