package io.specmesh.kafka;

import io.specmesh.kafka.schema.SchemaRegistryContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.stream.Stream;

/**
 * See https://www.testcontainers.org/test_framework_integration/manual_lifecycle_control/#singleton-containers
 */
abstract class AbstractContainerTest {
    protected AbstractContainerTest(){}
    static final String CFLT_VERSION = "6.2.7";
    static final Network network = Network.newNetwork();
    static KafkaContainer kafkaContainer;
    static SchemaRegistryContainer schemaRegistryContainer;

     static {

        try {
            kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:" + CFLT_VERSION))
                    .withNetwork(network)
                    .withStartupTimeout(Duration.ofSeconds(90));

            schemaRegistryContainer = new SchemaRegistryContainer(CFLT_VERSION)
                    .withNetwork(network)
                    .withKafka(kafkaContainer)
                    .withStartupTimeout(Duration.ofSeconds(90));

            Startables
                    .deepStart(Stream.of(kafkaContainer, schemaRegistryContainer))
                    .join();

        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

}
