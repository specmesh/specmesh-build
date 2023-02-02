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

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.CLIENT_ID_CONFIG;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.specmesh.apiparser.AsyncApiParser;
import io.specmesh.apiparser.model.ApiSpec;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class KafkaAPISpecFunctionalTest {

    private static final KafkaApiSpec API_SPEC = new KafkaApiSpec(getAPISpecFromResource());
    private static final String FOREIGN_DOMAIN = ".london.hammersmith.transport";
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
        final Map<String, Object> props = clientProperties();
        props.putAll(Provisioner.clientSaslAuthProperties(ADMIN_USER, ADMIN_USER + "-secret"));
        try (Admin adminClient = AdminClient.create(props)) {
            Provisioner.provisionTopics(API_SPEC, adminClient);
            Provisioner.provisionAcls(API_SPEC, adminClient);
        }
    }

    @Test
    public void shouldPublishAndConsumePublicTopics() {
        final List<NewTopic> newTopics = API_SPEC.listDomainOwnedTopics();
        final NewTopic publicTopic = newTopics.get(0);

        assertThat("Expected 'public'", publicTopic.name(), containsString("._public."));
        produceAndConsume(publicTopic.name(), API_SPEC.id(), API_SPEC.id());
    }

    @Test
    public void shouldPublishAndConsumeProtectedTopics() {
        final List<NewTopic> newTopics = API_SPEC.listDomainOwnedTopics();
        final NewTopic protectedTopic = newTopics.get(1);

        assertThat("Expected 'protected'", protectedTopic.name(), containsString("._protected."));
        produceAndConsume(protectedTopic.name(), API_SPEC.id(), API_SPEC.id());
    }

    @Test
    public void shouldPublishAndConsumePrivateTopics() {
        final List<NewTopic> newTopics = API_SPEC.listDomainOwnedTopics();
        final NewTopic privateTopic = newTopics.get(2);

        assertThat("Expected 'private'", privateTopic.name(), containsString("._private."));
        produceAndConsume(privateTopic.name(), API_SPEC.id(), API_SPEC.id());
    }

    @Test
    public void shouldConsumePublicTopicByForeignConsumer() {
        final List<NewTopic> newTopics = API_SPEC.listDomainOwnedTopics();
        final NewTopic publicTopic = newTopics.get(0);

        produceAndConsume(publicTopic.name(), API_SPEC.id(), FOREIGN_DOMAIN);
    }

    @Test
    public void shouldNotConsumeProtectedTopicByForeignConsumer() {
        final List<NewTopic> newTopics = API_SPEC.listDomainOwnedTopics();
        final NewTopic protectedTopic = newTopics.get(1);

        final Exception e =
                assertThrows(
                        TopicAuthorizationException.class,
                        () ->
                                produceAndConsume(
                                        protectedTopic.name(), API_SPEC.id(), FOREIGN_DOMAIN));

        assertThat(e.getMessage(), containsString("Not authorized to access topics"));
        assertThat(e.getMessage(), containsString(protectedTopic.name()));
    }

    @Test
    public void shouldGrantRestrictedAccessToProtectedTopic() {
        final List<NewTopic> newTopics = API_SPEC.listDomainOwnedTopics();
        final NewTopic publicTopic = newTopics.get(0);

        produceAndConsume(publicTopic.name(), API_SPEC.id(), SOME_OTHER_DOMAIN_ROOT);
    }

    private void produceAndConsume(
            final String topicName, final String producerDomain, final String consumerDomain) {
        // Given:
        try (KafkaConsumer<Long, String> domainConsumer = domainConsumer(consumerDomain);
                KafkaProducer<Long, String> domainProducer = domainProducer(producerDomain)) {

            domainConsumer.subscribe(List.of(topicName));
            domainConsumer.poll(Duration.ofSeconds(1));

            // When:
            domainProducer
                    .send(new ProducerRecord<>(topicName, 100L, "got value"))
                    .get(30, TimeUnit.SECONDS);

            final ConsumerRecords<Long, String> records =
                    domainConsumer.poll(Duration.of(30, TimeUnit.SECONDS.toChronoUnit()));

            // Then:
            assertThat("Didnt get Record", records.count(), is(1));
            assertThat(records.iterator().next().value(), is("got value"));
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            throw new RuntimeException(e);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new AssertionError(e);
        }
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

    private KafkaProducer<Long, String> domainProducer(final String domainId) {
        final Map<String, Object> props = clientProperties();
        props.putAll(Provisioner.clientSaslAuthProperties(domainId, domainId + "-secret"));
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, domainId + ".producer");

        return new KafkaProducer<>(props, Serdes.Long().serializer(), Serdes.String().serializer());
    }

    private KafkaConsumer<Long, String> domainConsumer(final String domainId) {
        final Map<String, Object> props = clientProperties();
        props.putAll(Provisioner.clientSaslAuthProperties(domainId, domainId + "-secret"));
        props.putAll(
                Map.of(
                        ConsumerConfig.CLIENT_ID_CONFIG,
                        domainId + ".consumer",
                        ConsumerConfig.GROUP_ID_CONFIG,
                        domainId + ".consumer-group",
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        "latest"));

        return new KafkaConsumer<>(
                props, Serdes.Long().deserializer(), Serdes.String().deserializer());
    }

    private static Map<String, Object> clientProperties() {
        final Map<String, Object> props = new HashMap<>();
        props.put(CLIENT_ID_CONFIG, API_SPEC.id());
        props.put(BOOTSTRAP_SERVERS_CONFIG, KAFKA_ENV.kafkaBootstrapServers());
        return props;
    }
}
