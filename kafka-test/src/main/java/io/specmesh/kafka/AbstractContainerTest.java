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


import io.specmesh.kafka.schema.SchemaRegistryContainer;
import java.time.Duration;
import java.util.Map;
import java.util.stream.Stream;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

/**
 * See
 * https://www.testcontainers.org/test_framework_integration/manual_lifecycle_control/#singleton-containers
 */
abstract class AbstractContainerTest {
    protected AbstractContainerTest() {
    }
    static final String CFLT_VERSION = "7.2.2";
    static final Network network = Network.newNetwork();
    static KafkaContainer kafkaContainer;
    static SchemaRegistryContainer schemaRegistryContainer;

    static {

        try {
            kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:" + CFLT_VERSION))
                    .withNetwork(network).withStartupTimeout(Duration.ofSeconds(90))
                    .withEnv(Provisioner.testAuthorizerConfig("simple.schema_demo", "simple.schema_demo-secret",
                            "foreignDomain", "foreignDomain-secret",
                            Map.of("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")));

            schemaRegistryContainer = new SchemaRegistryContainer(CFLT_VERSION).withNetwork(network)
                    .withKafka(kafkaContainer).withStartupTimeout(Duration.ofSeconds(90));

            Startables.deepStart(Stream.of(kafkaContainer, schemaRegistryContainer)).join();

        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

}
