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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.specmesh.test.TestSpecLoader;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
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
import org.junitpioneer.jupiter.cartesian.ArgumentSets;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.MethodFactory;

@SuppressFBWarnings(
        value = "IC_INIT_CIRCULARITY",
        justification = "shouldHaveInitializedEnumsCorrectly() proves this is false positive")
class KafkaAPISpecFunctionalTest {

    private static final KafkaApiSpec API_SPEC =
            TestSpecLoader.loadFromClassPath("apispec-functional-test-app.yaml");

    private enum Topic {
        PUBLIC(API_SPEC.listDomainOwnedTopics().get(0).name()),
        PROTECTED(API_SPEC.listDomainOwnedTopics().get(1).name()),
        PRIVATE(API_SPEC.listDomainOwnedTopics().get(2).name());

        final String topicName;

        Topic(final String name) {
            this.topicName = name;
        }
    }

    private enum Domain {
        /** The domain associated with the spec. */
        SELF(API_SPEC.id()),
        /** An unrelated domain. */
        UNRELATED(".london.hammersmith.transport"),
        /** A domain granted access to the protected topic. */
        LIMITED(".some.other.domain.root");

        final String domainId;

        Domain(final String name) {
            this.domainId = name;
        }
    }

    private static final String ADMIN_USER = "admin";

    @RegisterExtension
    private static final KafkaEnvironment KAFKA_ENV =
            DockerKafkaEnvironment.builder()
                    .withoutSchemaRegistry()
                    .withSaslAuthentication(
                            ADMIN_USER,
                            ADMIN_USER + "-secret",
                            Domain.SELF.domainId,
                            Domain.SELF.domainId + "-secret",
                            Domain.UNRELATED.domainId,
                            Domain.UNRELATED.domainId + "-secret",
                            Domain.LIMITED.domainId,
                            Domain.LIMITED.domainId + "-secret")
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
    void shouldHaveInitializedEnumsCorrectly() {
        assertThat(Topic.PUBLIC.topicName, is(API_SPEC.listDomainOwnedTopics().get(0).name()));
        assertThat(Topic.PROTECTED.topicName, is(API_SPEC.listDomainOwnedTopics().get(1).name()));
        assertThat(Topic.PRIVATE.topicName, is(API_SPEC.listDomainOwnedTopics().get(2).name()));
        assertThat(Domain.SELF.domainId, is(API_SPEC.id()));
    }

    @Test
    void shouldHavePickedRightTopicsOutOfSpec() {
        assertThat(Topic.PUBLIC.topicName, containsString("._public."));
        assertThat(Topic.PROTECTED.topicName, containsString("._protected."));
        assertThat(Topic.PRIVATE.topicName, containsString("._private."));
    }

    @CartesianTest
    @MethodFactory("testDimensions")
    void shouldHandle(final Topic topic, final Domain producerDomain, final Domain consumerDomain) {
        if (shouldSucceed(topic, producerDomain, consumerDomain)) {
            produceAndConsume(topic, producerDomain, consumerDomain);
        } else {
            final Exception e =
                    assertThrows(
                            TopicAuthorizationException.class,
                            () -> produceAndConsume(topic, producerDomain, consumerDomain));

            assertThat(e.getMessage(), containsString("Not authorized to access topics"));
            assertThat(e.getMessage(), containsString(topic.topicName));
        }
    }

    private void produceAndConsume(
            final Topic topic, final Domain producerDomain, final Domain consumerDomain) {
        // Given:
        try (KafkaConsumer<Long, String> domainConsumer = domainConsumer(consumerDomain);
                KafkaProducer<Long, String> domainProducer = domainProducer(producerDomain)) {

            domainConsumer.subscribe(List.of(topic.topicName));
            domainConsumer.poll(Duration.ofSeconds(1));

            // When:
            domainProducer
                    .send(new ProducerRecord<>(topic.topicName, 100L, "got value"))
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

    private KafkaProducer<Long, String> domainProducer(final Domain domain) {
        final Map<String, Object> props = clientProperties();
        props.putAll(
                Provisioner.clientSaslAuthProperties(domain.domainId, domain.domainId + "-secret"));
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, domain.domainId + ".producer");

        return new KafkaProducer<>(props, Serdes.Long().serializer(), Serdes.String().serializer());
    }

    private KafkaConsumer<Long, String> domainConsumer(final Domain domain) {
        final Map<String, Object> props = clientProperties();
        props.putAll(
                Provisioner.clientSaslAuthProperties(domain.domainId, domain.domainId + "-secret"));
        props.putAll(
                Map.of(
                        ConsumerConfig.CLIENT_ID_CONFIG,
                        domain.domainId + ".consumer",
                        ConsumerConfig.GROUP_ID_CONFIG,
                        domain.domainId + ".consumer-group",
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        "latest"));

        return new KafkaConsumer<>(
                props, Serdes.Long().deserializer(), Serdes.String().deserializer());
    }

    private static Map<String, Object> clientProperties() {
        final Map<String, Object> props = new HashMap<>();
        props.put(CLIENT_ID_CONFIG, Domain.SELF.domainId);
        props.put(BOOTSTRAP_SERVERS_CONFIG, KAFKA_ENV.kafkaBootstrapServers());
        return props;
    }

    private boolean shouldSucceed(
            final Topic topic, final Domain producerDomain, final Domain consumerDomain) {
        return canProduce(producerDomain) && canConsume(topic, consumerDomain);
    }

    private boolean canProduce(final Domain producerDomain) {
        return producerDomain == Domain.SELF;
    }

    private boolean canConsume(final Topic topic, final Domain consumerDomain) {
        switch (topic) {
            case PUBLIC:
                return true;
            case PROTECTED:
                return consumerDomain == Domain.SELF || consumerDomain == Domain.LIMITED;
            case PRIVATE:
                return consumerDomain == Domain.SELF;
            default:
                return false;
        }
    }

    @SuppressWarnings("unused") // Invoked by reflection
    protected static ArgumentSets testDimensions() {
        return ArgumentSets.argumentsForFirstParameter(Topic.values())
                .argumentsForNextParameter(Domain.values())
                .argumentsForNextParameter(Domain.values());
    }
}
