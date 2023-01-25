package io.specmesh.kafka.schema;


import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

/**
 * Test container for the Schema Registry
 */
public class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {

    private static final String SCHEMA_REGISTRY_DOCKER_IMAGE_NAME = "confluentinc/cp-schema-registry:6.0.2";
    private static final DockerImageName SCHEMA_REGISTRY_DOCKER_IMAGE = DockerImageName
            .parse(SCHEMA_REGISTRY_DOCKER_IMAGE_NAME);

    /**
     * Port the SR will listen on.
     */
    public static final int SCHEMA_REGISTRY_PORT = 8081;

    /**
     * @param version
     *            docker image version of schema registry
     */
    public SchemaRegistryContainer(final String version) {
        super(SCHEMA_REGISTRY_DOCKER_IMAGE.withTag(version));
        withExposedPorts(SCHEMA_REGISTRY_PORT);
    }

    /**
     * Link to Kafka container
     *
     * @param kafka
     *            kafka container
     * @return self.
     */
    public SchemaRegistryContainer withKafka(final KafkaContainer kafka) {
        return withKafka(kafka.getNetwork(), kafka.getNetworkAliases().get(0) + ":9092");
    }

    /**
     * Link to Network with Kafka
     *
     * @param network
     *            the network Kafka is running on
     * @param bootstrapServers
     *            the Kafka bootstrap servers
     * @return self.
     */
    public SchemaRegistryContainer withKafka(final Network network, final String bootstrapServers) {
        withNetwork(network);
        withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry");
        withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:" + SCHEMA_REGISTRY_PORT);
        withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://" + bootstrapServers);
        return self();
    }

    /**
     * @return Url of the SR.
     */
    public String getUrl() {
        return "http://" + getHost() + ":" + getMappedPort(SCHEMA_REGISTRY_PORT);
    }
}