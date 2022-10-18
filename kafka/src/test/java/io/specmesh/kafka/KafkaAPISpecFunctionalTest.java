package io.specmesh.kafka;


import io.specmesh.apiparser.AsyncApiParser;
import io.specmesh.apiparser.model.ApiSpec;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.serialization.Serdes;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KafkaAPISpecFunctionalTest {
    public static final String FOREIGN_DOMAIN = "london.hammersmith.transport";
    public static final int WAIT = 10;


    final static private KafkaApiSpec apiSpec = new KafkaApiSpec(getAPISpecFromResource());

    // CHECKSTYLE_RULES.OFF: VisibilityModifier
    @Container
    public static KafkaContainer kafka = getKafkaContainer();

    private AdminClient adminClient;

    @BeforeEach
    public void createAllTheThings() {
        System.out.println("createAllTheThings BROKER URL:" + kafka.getBootstrapServers());
        adminClient = AdminClient.create(getClientProperties());
    }


    @Order(1)
    @Test
    public void shouldCreateDomainTopicsWithACLs() throws Exception {
        final List<NewTopic> newTopics = apiSpec.listDomainOwnedTopics();
        adminClient.createTopics(newTopics).all().get(WAIT, TimeUnit.SECONDS);
        System.out.println("CREATED TOPICS: " + newTopics.size());
        final List<AclBinding> aclBindings = apiSpec.listACLsForDomainOwnedTopics();
        adminClient.createAcls(aclBindings).all().get(WAIT, TimeUnit.SECONDS);
    }

    @Order(2)
    @Test
    public void shouldPublishToMyPublicTopic() throws Exception {
        List<NewTopic> newTopics = apiSpec.listDomainOwnedTopics();

        NewTopic publicTopic = newTopics.get(0);
        NewTopic protectedTopic = newTopics.get(1);
        NewTopic privateTopic = newTopics.get(2);

        assertThat("Expected 'public'", publicTopic.name(), containsString(".public."));
        assertThat("Expected 'protected'", protectedTopic.name(), containsString(".protected."));
        assertThat("Expected 'private'", privateTopic.name(), containsString(".private."));


        KafkaProducer<Long, String> domainProducer = getDomainProducer(apiSpec.id());

        domainProducer.send(new ProducerRecord(publicTopic.name(), 100L, "got value")).get(WAIT, TimeUnit.SECONDS);

        domainProducer.send(new ProducerRecord(protectedTopic.name(), 200L, "got value")).get(WAIT, TimeUnit.SECONDS);

        domainProducer.send(new ProducerRecord(privateTopic.name(), 300L, "got value")).get(WAIT, TimeUnit.SECONDS);

        domainProducer.close();

    }

    @Order(3)
    @Test
    public void shouldConsumeMyPublicTopic() throws Exception {

        List<NewTopic> newTopics = apiSpec.listDomainOwnedTopics();
        NewTopic publicTopic = newTopics.get(0);

        assertThat(publicTopic.name(), containsString(".public."));

        KafkaConsumer<Long, String> domainConsumer = getDomainConsumer(apiSpec.id());

        domainConsumer.subscribe(Collections.singleton(publicTopic.name()));
        ConsumerRecords<Long, String> records = domainConsumer.poll(Duration.of(WAIT, TimeUnit.SECONDS.toChronoUnit()));

        domainConsumer.close();

        assertThat("Didnt get Record", records.count(), is(1));
    }

    @Order(4)
    @Test
    public void shouldConsumeMyProtectedTopic() throws Exception {

        List<NewTopic> newTopics = apiSpec.listDomainOwnedTopics();
        NewTopic protectedTopic = newTopics.get(1);
        assertThat(protectedTopic.name(), containsString(".protected."));


        KafkaConsumer<Long, String> domainConsumer = getDomainConsumer(apiSpec.id());

        domainConsumer.subscribe(Collections.singleton(protectedTopic.name()));
        ConsumerRecords<Long, String> records = domainConsumer.poll(Duration.of(WAIT, TimeUnit.SECONDS.toChronoUnit()));

        domainConsumer.close();

        assertThat("Didnt get Record", records.count(), is(1));
    }

    @Order(5)
    @Test
    public void shouldConsumeMyPrivateTopic() throws Exception {

        List<NewTopic> newTopics = apiSpec.listDomainOwnedTopics();
        NewTopic protectedTopic = newTopics.get(2);
        assertThat(protectedTopic.name(), containsString(".private."));

        KafkaConsumer<Long, String> domainConsumer = getDomainConsumer(apiSpec.id());

        domainConsumer.subscribe(Collections.singleton(protectedTopic.name()));
        ConsumerRecords<Long, String> records = domainConsumer.poll(Duration.of(WAIT, TimeUnit.SECONDS.toChronoUnit()));
        domainConsumer.close();

        assertThat("Didnt get Record", records.count(), is(1));
    }

    @Order(6)
    @Test
    public void shouldConsumePublicTopicByForeignConsumer() throws Exception {

        List<NewTopic> newTopics = apiSpec.listDomainOwnedTopics();
        NewTopic publicTopic = newTopics.get(0);

        assertThat(publicTopic.name(), containsString(".public."));


        KafkaConsumer<Long, String> foreignConsumer = getDomainConsumer(FOREIGN_DOMAIN);
        foreignConsumer.subscribe(Collections.singleton(publicTopic.name()));
        ConsumerRecords<Long, String> consumerRecords = foreignConsumer.poll(Duration.of(WAIT, TimeUnit.SECONDS.toChronoUnit()));
        foreignConsumer.close();

        assertThat("Didnt get Record", consumerRecords.count(), is(1));
    }

    @Order(7)
    @Test
    public void shouldNotConsumeProtectedTopicByForeignConsumer() throws Exception {

        List<NewTopic> newTopics = apiSpec.listDomainOwnedTopics();
        NewTopic protectedTopic = newTopics.get(1);

        assertThat(protectedTopic.name(), containsString(".protected."));

        KafkaConsumer<Long, String> foreignConsumer = getDomainConsumer(FOREIGN_DOMAIN);
        foreignConsumer.subscribe(Collections.singleton(protectedTopic.name()));

        TopicAuthorizationException throwable = assertThrows(TopicAuthorizationException.class, () -> {
                    foreignConsumer.poll(Duration.of(WAIT, TimeUnit.SECONDS.toChronoUnit()));
                }

        );
        assertThat(throwable.toString(), containsString("org.apache.kafka.common.errors.TopicAuthorizationException: Not authorized to access topics: [london.hammersmith.olympia.bigdatalondon.protected.retail.subway.food.purchase]"));
    }

    private Properties cloneProperties(Properties adminClientProperties, Map<String, String> entries) {
        Properties results = new Properties();
        results.putAll(adminClientProperties);
        results.putAll(entries);
        return results;
    }

    private static ApiSpec getAPISpecFromResource() {
        try {
            return new AsyncApiParser().loadResource(KafkaAPISpecFunctionalTest.class.getClassLoader().getResourceAsStream("bigdatalondon-api.yaml"));
        } catch (Throwable t) {
            throw new RuntimeException("Failed to load test resource", t);
        }
    }

    private KafkaProducer<Long, String> getDomainProducer(final String domainId) {
        return new KafkaProducer<>(
                cloneProperties(getClientProperties(),
                        Map.of(
                                AdminClientConfig.CLIENT_ID_CONFIG, domainId + ".producer",
                                "sasl.jaas.config", String.format("org.apache.kafka.common.security.plain.PlainLoginModule required " +
                                        "   username=\"domain-%s\" " +
                                        "   password=\"%s-secret\";", domainId, domainId))
                ),
                Serdes.Long().serializer(),
                Serdes.String().serializer());
    }

    private KafkaConsumer<Long, String> getDomainConsumer(final String domainId) {
        return new KafkaConsumer<>(
                cloneProperties(getClientProperties(),
                        Map.of(
                                ConsumerConfig.CLIENT_ID_CONFIG, domainId + ".consumer",
                                ConsumerConfig.GROUP_ID_CONFIG, domainId + ".consumer-group",
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                                "sasl.jaas.config", String.format("org.apache.kafka.common.security.plain.PlainLoginModule required " +
                                        "   username=\"domain-%s\" " +
                                        "   password=\"%s-secret\";", domainId, domainId)
                        )
                ),
                Serdes.Long().deserializer(),
                Serdes.String().deserializer());
    }

    @NotNull
    private static Properties getClientProperties() {
        final Properties adminClientProperties = new Properties();
        adminClientProperties.put(AdminClientConfig.CLIENT_ID_CONFIG, apiSpec.id());
        adminClientProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        adminClientProperties.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        adminClientProperties.put("sasl.mechanism", "PLAIN");
        adminClientProperties.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "   username=\"admin\" password=\"admin-secret\";");
        return adminClientProperties;
    }


    private static KafkaContainer getKafkaContainer() {
        final Map<String, String> env = new LinkedHashMap<>();
        env.put("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
        env.put("KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND", "true");
        env.put("KAFKA_AUTHORIZER_CLASS_NAME", "kafka.security.authorizer.AclAuthorizer");
        env.put("KAFKA_SUPER_USERS", "User:OnlySuperUser");
        env.put("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN,SASL_PLAINTEXT");


        env.put("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "BROKER:PLAINTEXT,PLAINTEXT:SASL_PLAINTEXT");
        env.put("KAFKA_LISTENER_NAME_PLAINTEXT_SASL_ENABLED_MECHANISMS", "PLAIN");
        env.put("KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG", "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "username=\"admin\" " +
                "password=\"admin-secret\" " +
                "user_admin=\"admin-secret\" " +
                String.format("user_domain-%s=\"%s-secret\" ", apiSpec.id(), apiSpec.id()) +
                String.format("user_domain-%s=\"%s-secret\" ", FOREIGN_DOMAIN, FOREIGN_DOMAIN) +
                ";"
        );

        env.put("KAFKA_SASL_JAAS_CONFIG", "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "username=\"admin\" " +
                "password=\"admin-secret\";");
        return new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
                .withStartupAttempts(3)
                .withEnv(env)
                ;
    }
}
