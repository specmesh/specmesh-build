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
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import io.specmesh.kafka.DockerKafkaEnvironment;
import io.specmesh.kafka.KafkaEnvironment;
import io.specmesh.kafka.provision.TopicProvisioner.Topic;
import java.util.stream.Collectors;
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

    @Test
    void shouldProvisionTopicsAndAclResourcesWithNestedSchemasAndRepublishCorrectly() {
        // Given:
        final Provision provision = new Provision();

        new CommandLine(provision)
                .parseArgs(
                        ("--bootstrap-server "
                                        + KAFKA_ENV.kafkaBootstrapServers()
                                        + " --spec nested_schema_demo-api.yaml"
                                        + " --username admin"
                                        + " --secret admin-secret"
                                        + " --schema-registry "
                                        + KAFKA_ENV.schemaRegistryServer()
                                        + " --schema-path ./resources")
                                .split(" "));

        // When:
        final var status = provision.run();

        // Then:
        assertThat(status.failed(), is(false));

        assertThat(
                status.topics().stream().map(Topic::name).collect(Collectors.toSet()),
                is(
                        containsInAnyOrder(
                                "simple.schema_demo._public.super_user_signed_up",
                                "simple.schema_demo._public.user_signed_up")));

        assertThat(status.acls(), hasSize(10));
        assertThat(status.schemas(), hasSize(2));

        // When:
        final var statusRepublish = provision.run();

        // Then:
        assertThat(statusRepublish.failed(), is(false));
        assertThat(statusRepublish.topics(), is(empty()));
        assertThat(statusRepublish.acls(), is(empty()));
        assertThat(
                "should be empty, but is 1 due to version number difference when comparing"
                        + " references",
                statusRepublish.schemas(),
                hasSize(1));
    }
}
