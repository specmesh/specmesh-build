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
import static org.apache.kafka.common.acl.AclOperation.ALL;
import static org.apache.kafka.common.acl.AclOperation.IDEMPOTENT_WRITE;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.common.resource.PatternType.LITERAL;
import static org.apache.kafka.common.resource.PatternType.PREFIXED;
import static org.apache.kafka.common.resource.Resource.CLUSTER_NAME;
import static org.apache.kafka.common.resource.ResourceType.CLUSTER;
import static org.apache.kafka.common.resource.ResourceType.GROUP;
import static org.apache.kafka.common.resource.ResourceType.TRANSACTIONAL_ID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.specmesh.kafka.provision.ProvisionAcls;
import io.specmesh.kafka.provision.ProvisionTopics;
import io.specmesh.kafka.provision.Provisioner;
import io.specmesh.test.TestSpecLoader;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.function.Executable;
import org.junitpioneer.jupiter.cartesian.CartesianTest;

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
                    .withKafkaAcls(aclsForOtherDomain(Domain.LIMITED))
                    .withKafkaAcls(aclsForOtherDomain(Domain.UNRELATED))
                    .build();

    @BeforeAll
    static void setUp() {
        try (Admin adminClient = KAFKA_ENV.adminClient()) {
            ProvisionTopics.provision(false, API_SPEC, adminClient);
            ProvisionAcls.provision(false, API_SPEC, adminClient);
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

    @CartesianTest(name = "topic: {0}, producer: {1}, consumer: {2}")
    void shouldHaveCorrectProduceAndConsumeAcls(
            @CartesianTest.Enum final Topic topic,
            @CartesianTest.Enum final Domain producerDomain,
            @CartesianTest.Enum final Domain consumerDomain) {
        final boolean canConsume = canConsume(topic, consumerDomain);
        final boolean canProduce = canProduce(producerDomain);

        try (Consumer<Long, String> domainConsumer = domainConsumer(consumerDomain);
                Producer<Long, String> domainProducer = domainProducer(producerDomain)) {

            domainProducer.initTransactions();

            domainConsumer.subscribe(List.of(topic.topicName));

            final Executable poll = () -> domainConsumer.poll(Duration.ofMillis(500));
            if (canConsume) {
                poll.execute();
            } else {
                final Exception e = assertThrows(TopicAuthorizationException.class, poll);
                assertThat((Throwable) e, instanceOf(TopicAuthorizationException.class));
            }

            domainProducer.beginTransaction();

            final Executable send =
                    () ->
                            domainProducer
                                    .send(new ProducerRecord<>(topic.topicName, 100L, "got value"))
                                    .get(30, TimeUnit.SECONDS);

            if (canProduce) {
                send.execute();
                domainProducer.commitTransaction();
            } else {
                final Throwable e = assertThrows(ExecutionException.class, send).getCause();
                assertThat(e, instanceOf(TopicAuthorizationException.class));
                domainProducer.abortTransaction();
            }

            if (canConsume && canProduce) {
                final ConsumerRecords<Long, String> records =
                        domainConsumer.poll(Duration.ofSeconds(30));

                assertThat("Didnt get Record", records.count(), is(1));
                assertThat(records.iterator().next().value(), is("got value"));
            }
        } catch (Throwable e) {
            if (e instanceof Error) {
                throw (Error) e;
            }
            throw new AssertionError(e);
        }
    }

    @CartesianTest(name = "topic: {0}, domain: {1}")
    void shouldHaveCorrectTopicCreationAcls(
            @CartesianTest.Enum final Topic topic, @CartesianTest.Enum final Domain domain)
            throws Throwable {

        final boolean shouldSucceed = topic == Topic.PRIVATE && domain == Domain.SELF;
        final NewTopic newTopic =
                new NewTopic(topic.topicName + "." + UUID.randomUUID(), 1, (short) 1);

        try (Admin adminClient = adminClient(domain)) {
            final KafkaFuture<Void> f = adminClient.createTopics(List.of(newTopic)).all();
            final Executable executable = () -> f.get(30, TimeUnit.SECONDS);

            if (shouldSucceed) {
                executable.execute();
            } else {
                final Throwable cause =
                        assertThrows(ExecutionException.class, executable).getCause();
                assertThat(cause, is(instanceOf(TopicAuthorizationException.class)));
            }
        }
    }

    private Admin adminClient(final Domain domain) {
        final Map<String, Object> props = clientProperties();
        props.putAll(saslAuthProperties(domain.domainId));
        return AdminClient.create(props);
    }

    private Producer<Long, String> domainProducer(final Domain domain) {
        final Map<String, Object> props = clientProperties();
        props.putAll(saslAuthProperties(domain.domainId));
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, domain.domainId + ".txId");

        return new KafkaProducer<>(props, new LongSerializer(), new StringSerializer());
    }

    private Consumer<Long, String> domainConsumer(final Domain domain) {
        final Map<String, Object> props = clientProperties();
        props.putAll(saslAuthProperties(domain.domainId));
        props.putAll(
                Map.of(
                        ConsumerConfig.GROUP_ID_CONFIG,
                        domain.domainId,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        "latest"));

        return new KafkaConsumer<>(props, new LongDeserializer(), new StringDeserializer());
    }

    private static Map<String, Object> clientProperties() {
        final Map<String, Object> props = new HashMap<>();
        props.put(CLIENT_ID_CONFIG, Domain.SELF.domainId);
        props.put(BOOTSTRAP_SERVERS_CONFIG, KAFKA_ENV.kafkaBootstrapServers());
        return props;
    }

    private static Map<String, Object> saslAuthProperties(final String domain) {
        return Provisioner.clientSaslAuthProperties(domain, domain + "-secret");
    }

    private static boolean canProduce(final Domain producerDomain) {
        return producerDomain == Domain.SELF;
    }

    private static boolean canConsume(final Topic topic, final Domain consumerDomain) {
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

    private static Set<AclBinding> aclsForOtherDomain(final Domain domain) {
        final String principal = "User:" + domain.domainId;
        return Set.of(
                new AclBinding(
                        new ResourcePattern(CLUSTER, CLUSTER_NAME, LITERAL),
                        new AccessControlEntry(principal, "*", IDEMPOTENT_WRITE, ALLOW)),
                new AclBinding(
                        new ResourcePattern(GROUP, domain.domainId, LITERAL),
                        new AccessControlEntry(principal, "*", ALL, ALLOW)),
                new AclBinding(
                        new ResourcePattern(TRANSACTIONAL_ID, domain.domainId, PREFIXED),
                        new AccessControlEntry(principal, "*", ALL, ALLOW)));
    }
}
