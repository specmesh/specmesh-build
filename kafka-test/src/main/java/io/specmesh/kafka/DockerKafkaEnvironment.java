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

import static java.util.Objects.requireNonNull;

import io.specmesh.kafka.schema.SchemaRegistryContainer;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

/**
 * A test utility for bringing up Kafka and Schema Registry Docker containers.
 *
 * <p>Instantiate the Docker based Kafka environment in test cases using the Junit5
 * {@code @RegisterExtension}:
 *
 * <pre>{@code
 * @RegisterExtension
 * private static final KafkaEnvironment KAFKA_ENV = DockerKafkaEnvironment.builder()
 *  .withContainerStartUpAttempts(4)
 *  .build();
 * }</pre>
 *
 * The `KAFKA_ENV` can then be queried for the {@link #kafkaBootstrapServers() Kafka endpoint} and
 * ths {@link #schemeRegistryServer() Schema Registry endpoint}.
 */
public final class DockerKafkaEnvironment
        implements KafkaEnvironment,
                BeforeAllCallback,
                BeforeEachCallback,
                AfterEachCallback,
                AfterAllCallback {

    private final int startUpAttempts;
    private final Duration startUpTimeout;
    private final DockerImageName kafkaDockerImage;
    private final Map<String, String> kafkaEnv;
    private final DockerImageName srDockerImage;
    private final Map<String, String> srEnv;

    private Network network;
    private KafkaContainer kafkaBroker;
    private SchemaRegistryContainer schemaRegistry;
    private boolean invokedStatically = false;

    /**
     * @return returns a {@link Builder} instance to allow customisation of the environment.
     */
    public static Builder builder() {
        return new Builder();
    }

    private DockerKafkaEnvironment(
            final int startUpAttempts,
            final Duration startUpTimeout,
            final DockerImageName kafkaDockerImage,
            final Map<String, String> kafkaEnv,
            final DockerImageName srDockerImage,
            final Map<String, String> srEnv) {
        this.startUpTimeout = requireNonNull(startUpTimeout, "startUpTimeout");
        this.startUpAttempts = startUpAttempts;
        this.kafkaDockerImage = requireNonNull(kafkaDockerImage, "kafkaDockerImage");
        this.kafkaEnv = Map.copyOf(requireNonNull(kafkaEnv, "kafkaEnv"));
        this.srDockerImage = requireNonNull(srDockerImage, "srDockerImage");
        this.srEnv = Map.copyOf(requireNonNull(srEnv, "srEnv"));
        tearDown();
    }

    @Override
    public void beforeAll(final ExtensionContext context) {
        invokedStatically = true;
        setUp();
    }

    @Override
    public void beforeEach(final ExtensionContext context) {
        if (invokedStatically) {
            return;
        }

        setUp();
    }

    @Override
    public void afterEach(final ExtensionContext context) {
        if (invokedStatically) {
            return;
        }

        tearDown();
    }

    @Override
    public void afterAll(final ExtensionContext context) {
        tearDown();
    }

    @Override
    public String kafkaBootstrapServers() {
        return kafkaBroker.getBootstrapServers();
    }

    @Override
    public String schemeRegistryServer() {
        return schemaRegistry.hostNetworkUrl().toString();
    }

    private void setUp() {
        network = Network.newNetwork();

        kafkaBroker =
                new KafkaContainer(kafkaDockerImage)
                        .withNetwork(network)
                        .withNetworkAliases("kafka")
                        .withStartupAttempts(startUpAttempts)
                        .withStartupTimeout(startUpTimeout)
                        .withEnv(kafkaEnv);

        schemaRegistry =
                new SchemaRegistryContainer(srDockerImage)
                        .withKafka(kafkaBroker)
                        .withNetworkAliases("schema-registry")
                        .withStartupAttempts(startUpAttempts)
                        .withStartupTimeout(startUpTimeout)
                        .withEnv(srEnv);

        schemaRegistry.start();
    }

    private void tearDown() {
        if (schemaRegistry != null) {
            schemaRegistry.close();
            schemaRegistry = null;
        }

        if (kafkaBroker != null) {
            kafkaBroker.close();
            kafkaBroker = null;
        }

        if (network != null) {
            network.close();
            network = null;
        }

        invokedStatically = false;
    }

    /** Builder of {@link DockerKafkaEnvironment}. */
    public static final class Builder {

        private static final int DEFAULT_CONTAINER_STARTUP_ATTEMPTS = 3;
        private static final Duration DEFAULT_CONTAINER_STARTUP_TIMEOUT = Duration.ofSeconds(30);

        private static final String DEFAULT_KAFKA_DOCKER_IMAGE = "confluentinc/cp-kafka:7.3.1";
        private static final Map<String, String> DEFAULT_KAFKA_ENV =
                Map.of("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

        private static final String DEFAULT_SCHEMA_REG_IMAGE =
                "confluentinc/cp-schema-registry:7.3.1";

        private int startUpAttempts = DEFAULT_CONTAINER_STARTUP_ATTEMPTS;
        private Duration startUpTimeout = DEFAULT_CONTAINER_STARTUP_TIMEOUT;
        private DockerImageName kafkaDockerImage =
                DockerImageName.parse(DEFAULT_KAFKA_DOCKER_IMAGE);
        private final Map<String, String> kafkaEnv = new HashMap<>(DEFAULT_KAFKA_ENV);
        private DockerImageName srImage = DockerImageName.parse(DEFAULT_SCHEMA_REG_IMAGE);
        private final Map<String, String> srEnv = new HashMap<>();

        /**
         * Customise the startup count.
         *
         * @param count the new count.
         * @return self.
         */
        public Builder withContainerStartUpAttempts(final int count) {
            if (count <= 0) {
                throw new IllegalArgumentException(
                        "container startup attempts must be positive, but was: " + count);
            }
            this.startUpAttempts = count;
            return this;
        }

        /**
         * Customise the startup timeout.
         *
         * @param timeout the new timeout.
         * @return self.
         */
        public Builder withContainerStartUpTimeout(final Duration timeout) {
            this.startUpTimeout = requireNonNull(timeout, "timeout");
            return this;
        }

        /**
         * Customise the Docker image to use for Kafka.
         *
         * @param imageName the Docker image name.
         * @return self.
         */
        public Builder withKafkaImage(final String imageName) {
            this.kafkaDockerImage = DockerImageName.parse(imageName);
            return this;
        }

        /**
         * Add an environment variable to set on the Kafka container.
         *
         * @param key the environment key.
         * @param value the environment value.
         * @return self.
         */
        public Builder withKafkaEnv(final String key, final String value) {
            return withKafkaEnv(Map.of(key, value));
        }

        /**
         * Add environment variables to set on the Kafka container.
         *
         * @param env the environment variables to set.
         * @return self.
         */
        public Builder withKafkaEnv(final Map<String, String> env) {
            this.kafkaEnv.putAll(env);
            return this;
        }

        /**
         * Customise the Docker image to use for Schema Registry.
         *
         * @param imageName the Docker image name.
         * @return self.
         */
        public Builder withSchemaRegistryImage(final String imageName) {
            this.srImage = DockerImageName.parse(imageName);
            return this;
        }

        /**
         * Add an environment variable to set on the Schema Registry container.
         *
         * @param key the environment key.
         * @param value the environment value.
         * @return self.
         */
        public Builder withSchemaRegistryEnv(final String key, final String value) {
            return withSchemaRegistryEnv(Map.of(key, value));
        }

        /**
         * Add environment variables to set on the Schema Registry container.
         *
         * @param env the environment variables to set.
         * @return self.
         */
        public Builder withSchemaRegistryEnv(final Map<String, String> env) {
            this.kafkaEnv.putAll(env);
            return this;
        }

        /**
         * @return the new {@link DockerKafkaEnvironment} instance.
         */
        public DockerKafkaEnvironment build() {
            return new DockerKafkaEnvironment(
                    startUpAttempts, startUpTimeout, kafkaDockerImage, kafkaEnv, srImage, srEnv);
        }
    }
}
