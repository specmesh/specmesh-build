package io.specmesh.kafka.topic;

import io.specmesh.apiparser.AsyncApiParser;
import io.specmesh.apiparser.model.ApiSpec;
import io.specmesh.kafka.KafkaApiSpec;
import org.apache.kafka.clients.admin.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Testcontainers
public class TopicAPISpecFuncationalTest {

    // CHECKSTYLE_RULES.OFF: VisibilityModifier
    @Container
    public KafkaContainer kafka = getKafkaContainer();

    final static private KafkaApiSpec apiSpec = new KafkaApiSpec(getAPISpecFromResource());

    private AdminClient adminClient;

    private KafkaContainer getKafkaContainer() {
        final Map<String, String> env = new LinkedHashMap<>();
        env.put("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
        env.put("KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND", "true");

        return new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
                .withStartupAttempts(3)
                .withEnv(env);
    }

    @BeforeEach
    public void prepareForTheMagic() {
        System.out.println("BROKER URL:" + kafka.getBootstrapServers());

        final Properties adminClientProperties = new Properties();
        adminClientProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        adminClient = AdminClient.create(adminClientProperties);
    }

    @Test
    public void shouldCreateTopic() throws Exception {
        List<NewTopic> topics = apiSpec.listDomainOwnedTopics();
        List<TopicListing> currentTopics = adminClient.listTopics().listings().get()
        .stream().filter(topic -> topic.name().startsWith(apiSpec.id())).collect(Collectors.toList());

        System.out.println(apiSpec.listDomainOwnedTopics());

        final var result = adminClient.createTopics(topics);
    }

    private void generateTestTopics(int numberOfTopics) {
        String topicName = "test";
        NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);

    }

    private static ApiSpec getAPISpecFromResource() {
        try {
            return new AsyncApiParser().loadResource(TopicAPISpecFuncationalTest.class.getClassLoader()
            .getResourceAsStream("bigdatalondon-api.yaml"));
        } catch (Throwable t) {
            throw new RuntimeException("Failed to load test resource", t);
        }
    }
}
