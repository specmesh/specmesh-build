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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class KafkaAPISpecFunctionalTest {

    private static final String FOREIGN_DOMAIN = ".london.hammersmith.transport";
    private static final int WAIT = 10;

    private static final KafkaApiSpec API_SPEC = new KafkaApiSpec(getAPISpecFromResource());
    private static final String SOME_OTHER_DOMAIN_ROOT = ".some.other.domain.root";

    private static final String ADMIN_USER = "admin";

    @RegisterExtension
    private static final KafkaEnvironment KAFKA_ENV =
            DockerKafkaEnvironment.builder()
                    .withoutSchemaRegistry()
                    .withSaslAuthentication(
                            ADMIN_USER,
                            ADMIN_USER + "-secret",
                            API_SPEC.id(),
                            API_SPEC.id() + "-secret",
                            FOREIGN_DOMAIN,
                            FOREIGN_DOMAIN + "-secret",
                            SOME_OTHER_DOMAIN_ROOT,
                            SOME_OTHER_DOMAIN_ROOT + "-secret")
                    .withKafkaAcls()
                    .build();

    @BeforeAll
    static void setUp() {
        final Admin adminClient = AdminClient.create(getClientProperties());
        Provisioner.provisionTopics(API_SPEC, adminClient);
        Provisioner.provisionAcls(API_SPEC, adminClient);
    }

    @Order(1)
    @Test
    public void shouldPublishToMyPublicTopic() throws Exception {
        final List<NewTopic> newTopics = API_SPEC.listDomainOwnedTopics();

        final NewTopic publicTopic = newTopics.get(0);
        final NewTopic protectedTopic = newTopics.get(1);
        final NewTopic privateTopic = newTopics.get(2);

        assertThat("Expected 'public'", publicTopic.name(), containsString("._public."));
        assertThat("Expected 'protected'", protectedTopic.name(), containsString("._protected."));
        assertThat("Expected 'private'", privateTopic.name(), containsString("._private."));

        final KafkaProducer<Long, String> domainProducer = getDomainProducer(API_SPEC.id());

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

        final List<NewTopic> newTopics = API_SPEC.listDomainOwnedTopics();
        final NewTopic publicTopic = newTopics.get(0);

        assertThat(publicTopic.name(), containsString("._public."));

        final KafkaConsumer<Long, String> domainConsumer = getDomainConsumer(API_SPEC.id());

        domainConsumer.subscribe(Collections.singleton(publicTopic.name()));
        final ConsumerRecords<Long, String> records =
                domainConsumer.poll(Duration.of(WAIT, TimeUnit.SECONDS.toChronoUnit()));

        domainConsumer.close();

        assertThat("Didnt get Record", records.count(), is(1));
    }

    @Order(3)
    @Test
    public void shouldConsumeMyProtectedTopic() {

        final List<NewTopic> newTopics = API_SPEC.listDomainOwnedTopics();
        final NewTopic protectedTopic = newTopics.get(1);
        assertThat(protectedTopic.name(), containsString("._protected."));

        final KafkaConsumer<Long, String> domainConsumer = getDomainConsumer(API_SPEC.id());

        domainConsumer.subscribe(Collections.singleton(protectedTopic.name()));
        final ConsumerRecords<Long, String> records =
                domainConsumer.poll(Duration.of(WAIT, TimeUnit.SECONDS.toChronoUnit()));

        domainConsumer.close();

        assertThat("Didnt get Record", records.count(), is(1));
    }

    @Order(4)
    @Test
    public void shouldConsumeMyPrivateTopic() {

        final List<NewTopic> newTopics = API_SPEC.listDomainOwnedTopics();
        final NewTopic protectedTopic = newTopics.get(2);
        assertThat(protectedTopic.name(), containsString("._private."));

        final KafkaConsumer<Long, String> domainConsumer = getDomainConsumer(API_SPEC.id());

        domainConsumer.subscribe(Collections.singleton(protectedTopic.name()));
        final ConsumerRecords<Long, String> records =
                domainConsumer.poll(Duration.of(WAIT, TimeUnit.SECONDS.toChronoUnit()));
        domainConsumer.close();

        assertThat("Didnt get Record", records.count(), is(1));
    }

    @Order(5)
    @Test
    public void shouldConsumePublicTopicByForeignConsumer() {

        final List<NewTopic> newTopics = API_SPEC.listDomainOwnedTopics();
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

        final List<NewTopic> newTopics = API_SPEC.listDomainOwnedTopics();
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

        final List<NewTopic> newTopics = API_SPEC.listDomainOwnedTopics();
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
        adminClientProperties.put(AdminClientConfig.CLIENT_ID_CONFIG, API_SPEC.id());
        adminClientProperties.put(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_ENV.kafkaBootstrapServers());
        adminClientProperties.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        adminClientProperties.put("sasl.mechanism", "PLAIN");
        adminClientProperties.put(
                "sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                        + "   username=\""
                        + ADMIN_USER
                        + "\" password=\""
                        + ADMIN_USER
                        + "-secret\";");
        return adminClientProperties;
    }
}
