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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.specmesh.apiparser.AsyncApiParser;
import io.specmesh.apiparser.model.ApiSpec;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KafkaAPISpecFunctionalTest {
    public static final String FOREIGN_DOMAIN = ".london.hammersmith.transport";
    public static final int WAIT = 10;

    private static final KafkaApiSpec apiSpec = new KafkaApiSpec(getAPISpecFromResource());
    public static final String SOME_OTHER_DOMAIN_ROOT = ".some.other.domain.root";

    // CHECKSTYLE_RULES.OFF: VisibilityModifier
    @Container public static final KafkaContainer kafka = getKafkaContainer();

    public KafkaAPISpecFunctionalTest() {
        System.out.println("createAllTheThings BROKER URL:" + kafka.getBootstrapServers());
        final AdminClient adminClient = AdminClient.create(getClientProperties());
        try {
            Provisioner.provisionTopics(apiSpec, adminClient);
            Provisioner.provisionAcls(apiSpec, adminClient);
        } catch (Throwable t) {
            // note: I will look a repeatable ops/deltas in next PR
        }
    }

    @Order(1)
    @Test
    public void shouldPublishToMyPublicTopic() throws Exception {
        final List<NewTopic> newTopics = apiSpec.listDomainOwnedTopics();

        final NewTopic publicTopic = newTopics.get(0);
        final NewTopic protectedTopic = newTopics.get(1);
        final NewTopic privateTopic = newTopics.get(2);

        assertThat("Expected 'public'", publicTopic.name(), containsString("._public."));
        assertThat("Expected 'protected'", protectedTopic.name(), containsString("._protected."));
        assertThat("Expected 'private'", privateTopic.name(), containsString("._private."));

        final KafkaProducer<Long, String> domainProducer = getDomainProducer(apiSpec.id());

        domainProducer
                .send(new ProducerRecord<>(publicTopic.name(), 100L, "got value"))
                .get(WAIT, TimeUnit.SECONDS);

        domainProducer
                .send(new ProducerRecord<>(protectedTopic.name(), 200L, "got value"))
                .get(WAIT, TimeUnit.SECONDS);

        domainProducer
                .send(new ProducerRecord<>(privateTopic.name(), 300L, "got value"))
                .get(WAIT, TimeUnit.SECONDS);

        domainProducer.close();
    }

    @Order(2)
    @Test
    public void shouldConsumeMyPublicTopic() {

        final List<NewTopic> newTopics = apiSpec.listDomainOwnedTopics();
        final NewTopic publicTopic = newTopics.get(0);

        assertThat(publicTopic.name(), containsString("._public."));

        final KafkaConsumer<Long, String> domainConsumer = getDomainConsumer(apiSpec.id());

        domainConsumer.subscribe(Collections.singleton(publicTopic.name()));
        final ConsumerRecords<Long, String> records =
                domainConsumer.poll(Duration.of(WAIT, TimeUnit.SECONDS.toChronoUnit()));

        domainConsumer.close();

        assertThat("Didnt get Record", records.count(), is(1));
    }

    @Order(3)
    @Test
    public void shouldConsumeMyProtectedTopic() {

        final List<NewTopic> newTopics = apiSpec.listDomainOwnedTopics();
        final NewTopic protectedTopic = newTopics.get(1);
        assertThat(protectedTopic.name(), containsString("._protected."));

        final KafkaConsumer<Long, String> domainConsumer = getDomainConsumer(apiSpec.id());

        domainConsumer.subscribe(Collections.singleton(protectedTopic.name()));
        final ConsumerRecords<Long, String> records =
                domainConsumer.poll(Duration.of(WAIT, TimeUnit.SECONDS.toChronoUnit()));

        domainConsumer.close();

        assertThat("Didnt get Record", records.count(), is(1));
    }

    @Order(4)
    @Test
    public void shouldConsumeMyPrivateTopic() {

        final List<NewTopic> newTopics = apiSpec.listDomainOwnedTopics();
        final NewTopic protectedTopic = newTopics.get(2);
        assertThat(protectedTopic.name(), containsString("._private."));

        final KafkaConsumer<Long, String> domainConsumer = getDomainConsumer(apiSpec.id());

        domainConsumer.subscribe(Collections.singleton(protectedTopic.name()));
        final ConsumerRecords<Long, String> records =
                domainConsumer.poll(Duration.of(WAIT, TimeUnit.SECONDS.toChronoUnit()));
        domainConsumer.close();

        assertThat("Didnt get Record", records.count(), is(1));
    }

    @Order(5)
    @Test
    public void shouldConsumePublicTopicByForeignConsumer() {

        final List<NewTopic> newTopics = apiSpec.listDomainOwnedTopics();
        final NewTopic publicTopic = newTopics.get(0);

        assertThat(publicTopic.name(), containsString("._public."));

        final KafkaConsumer<Long, String> foreignConsumer = getDomainConsumer(FOREIGN_DOMAIN);
        foreignConsumer.subscribe(Collections.singleton(publicTopic.name()));
        final ConsumerRecords<Long, String> consumerRecords =
                foreignConsumer.poll(Duration.of(WAIT, TimeUnit.SECONDS.toChronoUnit()));
        foreignConsumer.close();

        assertThat("Didnt get Record", consumerRecords.count(), is(1));
    }

    @Order(6)
    @Test
    public void shouldNotConsumeProtectedTopicByForeignConsumer() {

        final List<NewTopic> newTopics = apiSpec.listDomainOwnedTopics();
        final NewTopic protectedTopic = newTopics.get(1);

        assertThat(protectedTopic.name(), containsString("._protected."));

        final KafkaConsumer<Long, String> foreignConsumer = getDomainConsumer(FOREIGN_DOMAIN);
        foreignConsumer.subscribe(Collections.singleton(protectedTopic.name()));

        final TopicAuthorizationException throwable =
                assertThrows(
                        TopicAuthorizationException.class,
                        () ->
                                foreignConsumer.poll(
                                        Duration.of(WAIT, TimeUnit.SECONDS.toChronoUnit())));
        assertThat(
                throwable.toString(),
                containsString(
                        "org.apache.kafka.common.errors.TopicAuthorizationException: Not authorized to access topics: "
                                + "[london.hammersmith.olympia.bigdatalondon._protected.retail.subway.food.purchase]"));
    }

    @Order(7)
    @Test
    public void shouldGrantRestrictedAccessToProtectedTopic() {

        final List<NewTopic> newTopics = apiSpec.listDomainOwnedTopics();
        final NewTopic newTopic = newTopics.get(1);

        assertThat(newTopic.name(), containsString("._protected."));

        final KafkaConsumer<Long, String> foreignConsumer =
                getDomainConsumer(SOME_OTHER_DOMAIN_ROOT);
        foreignConsumer.subscribe(Collections.singleton(newTopic.name()));
        final ConsumerRecords<Long, String> consumerRecords =
                foreignConsumer.poll(Duration.of(WAIT, TimeUnit.SECONDS.toChronoUnit()));
        foreignConsumer.close();

        assertThat("Didnt get Record", consumerRecords.count(), is(1));
    }

    private Properties cloneProperties(
            final Properties adminClientProperties, final Map<String, String> entries) {
        final Properties results = new Properties();
        results.putAll(adminClientProperties);
        results.putAll(entries);
        return results;
    }

    private static ApiSpec getAPISpecFromResource() {
        try {
            return new AsyncApiParser()
                    .loadResource(
                            KafkaAPISpecFunctionalTest.class
                                    .getClassLoader()
                                    .getResourceAsStream("apispec-functional-test-app.yaml"));
        } catch (Throwable t) {
            throw new RuntimeException("Failed to load test resource", t);
        }
    }

    private KafkaProducer<Long, String> getDomainProducer(final String domainId) {
        return new KafkaProducer<>(
                cloneProperties(
                        getClientProperties(),
                        Map.of(
                                AdminClientConfig.CLIENT_ID_CONFIG,
                                domainId + ".producer",
                                "sasl.jaas.config",
                                String.format(
                                        "org.apache.kafka.common.security.plain.PlainLoginModule required "
                                                + "   username=\"%s\" "
                                                + "   password=\"%s-secret\";",
                                        domainId, domainId))),
                Serdes.Long().serializer(),
                Serdes.String().serializer());
    }

    private KafkaConsumer<Long, String> getDomainConsumer(final String domainId) {
        return new KafkaConsumer<>(
                cloneProperties(
                        getClientProperties(),
                        Map.of(
                                ConsumerConfig.CLIENT_ID_CONFIG,
                                domainId + ".consumer",
                                ConsumerConfig.GROUP_ID_CONFIG,
                                domainId + ".consumer-group",
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                                "earliest",
                                "sasl.jaas.config",
                                String.format(
                                        "org.apache.kafka.common.security.plain.PlainLoginModule required "
                                                + "   username=\"%s\" password=\"%s-secret\";",
                                        domainId, domainId))),
                Serdes.Long().deserializer(),
                Serdes.String().deserializer());
    }

    private static Properties getClientProperties() {
        final Properties adminClientProperties = new Properties();
        adminClientProperties.put(AdminClientConfig.CLIENT_ID_CONFIG, apiSpec.id());
        adminClientProperties.put(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        adminClientProperties.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        adminClientProperties.put("sasl.mechanism", "PLAIN");
        adminClientProperties.put(
                "sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                        + "   username=\"admin\" password=\"admin-secret\";");
        return adminClientProperties;
    }

    private static KafkaContainer getKafkaContainer() {
        final Map<String, String> env = new LinkedHashMap<>();
        env.put("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
        env.put("KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND", "true");
        env.put("KAFKA_AUTHORIZER_CLASS_NAME", "kafka.security.authorizer.AclAuthorizer");
        env.put("KAFKA_SUPER_USERS", "User:OnlySuperUser");
        env.put("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN,SASL_PLAINTEXT");

        env.put(
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                "BROKER:PLAINTEXT,PLAINTEXT:SASL_PLAINTEXT");
        env.put("KAFKA_LISTENER_NAME_PLAINTEXT_SASL_ENABLED_MECHANISMS", "PLAIN");
        env.put(
                "KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG",
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                        + "username=\"admin\" "
                        + "password=\"admin-secret\" "
                        + "user_admin=\"admin-secret\" "
                        + String.format("user_%s=\"%s-secret\" ", apiSpec.id(), apiSpec.id())
                        + String.format("user_%s=\"%s-secret\" ", FOREIGN_DOMAIN, FOREIGN_DOMAIN)
                        + String.format(
                                "user_%s=\"%s-secret\";",
                                SOME_OTHER_DOMAIN_ROOT, SOME_OTHER_DOMAIN_ROOT));

        env.put(
                "KAFKA_SASL_JAAS_CONFIG",
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                        + "username=\"admin\" "
                        + "password=\"admin-secret\";");
        return new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.2.2"))
                .withStartupAttempts(3)
                .withEnv(env);
    }

    @AfterAll
    public static void stopThings() {
        kafka.stop();
    }
}
