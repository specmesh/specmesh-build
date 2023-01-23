package io.specmesh.kafka.schema;


import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

public class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {

    private static final String SCHEMA_REGISTRY_DOCKER_IMAGE_NAME = "confluentinc/cp-schema-registry:6.0.2";
    private static final DockerImageName SCHEMA_REGISTRY_DOCKER_IMAGE = DockerImageName
            .parse(SCHEMA_REGISTRY_DOCKER_IMAGE_NAME);
    public static final int SCHEMA_REGISTRY_PORT = 8081;

    public SchemaRegistryContainer(final String version) {
        super(SCHEMA_REGISTRY_DOCKER_IMAGE.withTag(version));
        withExposedPorts(SCHEMA_REGISTRY_PORT);
    }

    public SchemaRegistryContainer withKafka(final KafkaContainer kafka) {
        return withKafka(kafka.getNetwork(), kafka.getNetworkAliases().get(0) + ":9092");
    }

    public SchemaRegistryContainer withKafka(final Network network, final String bootstrapServers) {
        withNetwork(network);
        withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry");
        withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:" + SCHEMA_REGISTRY_PORT);
        withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://" + bootstrapServers);
        return self();
    }

    public String getUrl() {
        return "http://" + getHost() + ":" + getMappedPort(SCHEMA_REGISTRY_PORT);
    }
}
