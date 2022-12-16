// CHECKSTYLE_RULES.OFF: FinalLocalVariable
// CHECKSTYLE_RULES.OFF: FinalParameters
package io.specmesh.kafka;


import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.Sets;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class SASLPlainPrincipleTest {
    public static final String DOMAIN_ROOT = "simple.streetlights";
    public static final String PUBLIC_LIGHT_MEASURED = ".public.light.measured";
    public static final String PRIVATE_LIGHT_EVENTS = ".private.light.events";
    public static final String FOREIGN_DOMAIN = "london.hammersmith.transport";
    // CHECKSTYLE_RULES.OFF: VisibilityModifier
    @Container
    public static final KafkaContainer kafka = getKafkaContainer();

    private static KafkaContainer getKafkaContainer() {
        final Map<String, String> env = new LinkedHashMap<>();
        env.put("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
        env.put("KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND", "true");
        env.put("KAFKA_SUPER_USERS", "User:OnlySuperUser");
        env.put("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN,SASL_PLAINTEXT");


        env.put("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "BROKER:PLAINTEXT,PLAINTEXT:SASL_PLAINTEXT");
        env.put("KAFKA_LISTENER_NAME_PLAINTEXT_SASL_ENABLED_MECHANISMS", "PLAIN");
        env.put("KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG", "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "username=\"admin\" " +
                "password=\"admin-secret\" " +
                "user_admin=\"admin-secret\" " +
                String.format("user_%s=\"%s-secret\" ", DOMAIN_ROOT, DOMAIN_ROOT) +
                String.format("user_%s_producer=\"%s_producer-secret\" ", DOMAIN_ROOT, DOMAIN_ROOT) +
                String.format("user_%s_consumer=\"%s_consumer-secret\" ", DOMAIN_ROOT, DOMAIN_ROOT) +
                String.format("user_%s_producer=\"%s_producer-secret\" ", FOREIGN_DOMAIN, FOREIGN_DOMAIN) +
                String.format("user_%s_consumer=\"%s_consumer-secret\";", FOREIGN_DOMAIN, FOREIGN_DOMAIN)

        );

        env.put("KAFKA_SASL_JAAS_CONFIG", "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "username=\"admin\" " +
                "password=\"admin-secret\";");
        return new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
                .withStartupAttempts(3)
                .withEnv(env)
                ;
    }

    private AdminClient adminClient;
    private KafkaProducer domainProducer;
    private KafkaConsumer<Long, String> domainConsumer;
    private KafkaConsumer<Long, String> foreignConsumer;

    @BeforeEach
    public void createAllTheThings() {

        final Properties adminClientProperties = new Properties();
        adminClientProperties.put(AdminClientConfig.CLIENT_ID_CONFIG, DOMAIN_ROOT);
        adminClientProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        adminClientProperties.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        adminClientProperties.put("sasl.mechanism", "PLAIN");
        adminClientProperties.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "   username=\"admin\" password=\"admin-secret\";");

        adminClient = AdminClient.create(adminClientProperties);
        CreateTopicsResult topics = adminClient.createTopics(
                Sets.newHashSet(
                        new NewTopic(
                                DOMAIN_ROOT + PUBLIC_LIGHT_MEASURED,
                                1,
                                Short.parseShort("1")),
                        new NewTopic(
                                DOMAIN_ROOT + PRIVATE_LIGHT_EVENTS,
                                1,
                                Short.parseShort("1")),
                        new NewTopic(
                                ".london.hammersmith.transport.public.tube", 1, Short.parseShort("1")
                        )
                ));

        topics.values().values().forEach(f -> {
            try {
                f.get();
            } catch (ExecutionException | InterruptedException | RuntimeException e) {
                throw new RuntimeException(e);
            }
        });

        domainProducer =
                new KafkaProducer<>(
                        cloneProperties(adminClientProperties, Map.of(
                                AdminClientConfig.CLIENT_ID_CONFIG, DOMAIN_ROOT + ".producer",
                                "sasl.jaas.config", String.format("org.apache.kafka.common.security.plain.PlainLoginModule required " +
                                        "   username=\"%s_producer\" " +
                                        "   password=\"%s_producer-secret\";", DOMAIN_ROOT, DOMAIN_ROOT)
                        )),
                        Serdes.Long().serializer(),
                        Serdes.String().serializer());

        domainConsumer = new KafkaConsumer<>(
                cloneProperties(adminClientProperties,
                        Map.of(
                                ConsumerConfig.CLIENT_ID_CONFIG, DOMAIN_ROOT + ".consumer",
                                ConsumerConfig.GROUP_ID_CONFIG, DOMAIN_ROOT + ".consumer-group",
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                                "sasl.jaas.config", String.format("org.apache.kafka.common.security.plain.PlainLoginModule required " +
                                        "   username=\"%s_consumer\" " +
                                        "   password=\"%s_consumer-secret\";", DOMAIN_ROOT, DOMAIN_ROOT)
                        )
                ),
                Serdes.Long().deserializer(),
                Serdes.String().deserializer());

        foreignConsumer = new KafkaConsumer<>(
                cloneProperties(adminClientProperties,
                        Map.of(
                                ConsumerConfig.CLIENT_ID_CONFIG, FOREIGN_DOMAIN + ".consumer",
                                ConsumerConfig.GROUP_ID_CONFIG, FOREIGN_DOMAIN + ".consumer-group",
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                                "sasl.jaas.config", String.format("org.apache.kafka.common.security.plain.PlainLoginModule required " +
                                        "   username=\"%s_consumer\" " +
                                        "   password=\"%s_consumer-secret\";", FOREIGN_DOMAIN, FOREIGN_DOMAIN)
                        )),
                Serdes.Long().deserializer(),
                Serdes.String().deserializer());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldPubSubStuff() throws Exception {

        domainProducer
                .send(new ProducerRecord(DOMAIN_ROOT + PUBLIC_LIGHT_MEASURED, 100L, "got value"))
                .get();

        domainConsumer.subscribe(Collections.singleton(DOMAIN_ROOT + PUBLIC_LIGHT_MEASURED));
        ConsumerRecords<Long, String> poll = domainConsumer.poll(Duration.of(30, TimeUnit.SECONDS.toChronoUnit()));

        assertThat("Didnt get Record", poll.count(), is(1));

        foreignConsumer.subscribe(Collections.singleton(DOMAIN_ROOT + PUBLIC_LIGHT_MEASURED));
        ConsumerRecords<Long, String> pollForeign = foreignConsumer.poll(Duration.of(30, TimeUnit.SECONDS.toChronoUnit()));

        assertThat("Didnt get Record", pollForeign.count(), is(1));
    }

    private Properties cloneProperties(Properties adminClientProperties, Map<String, String> entries) {
        Properties results = new Properties();
        results.putAll(adminClientProperties);
        results.putAll(entries);
        return results;
    }

    @AfterAll
    public static void stopThings() {
        kafka.stop();
    }
}
