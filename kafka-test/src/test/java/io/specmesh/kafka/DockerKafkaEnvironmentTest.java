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

import java.time.Duration;
import java.util.function.Consumer;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("ContainerisedTest")
class DockerKafkaEnvironmentTest {

    private static final int STARTUP_ATTEMPTS = 1;
    private static final Duration STARTUP_DURATION = Duration.ofSeconds(15);

    @Test
    void shouldWorkWithoutSasl() {
        assertStarts(builder -> {});
    }

    @Test
    void shouldWorkWithSasl() {
        assertStarts(
                builder -> builder.withSaslAuthentication("admin", "admin-secret").withKafkaAcls());
    }

    private void assertStarts(final Consumer<DockerKafkaEnvironment.Builder> customizer) {
        final DockerKafkaEnvironment.Builder builder =
                DockerKafkaEnvironment.builder()
                        .withContainerStartUpAttempts(STARTUP_ATTEMPTS)
                        .withContainerStartUpTimeout(STARTUP_DURATION);

        customizer.accept(builder);

        try (DockerKafkaEnvironment kafkaEnvironment = builder.build()) {
            try {
                // When:
                kafkaEnvironment.start();

                // Then: did not throw
            } catch (Exception e) {
                System.out.println("----------Kafka Logs---------------");
                System.out.println(kafkaEnvironment.kafkaLogs());
                System.out.println("----------Schema Registry Logs---------------");
                System.out.println(kafkaEnvironment.schemaRegistryLogs());
                throw e;
            }
        }
    }
}
