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

package io.specmesh.cli;

import static io.specmesh.cli.util.AssertEventually.assertThatEventually;
import static io.specmesh.kafka.Clients.producerProperties;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.specmesh.kafka.Clients;
import io.specmesh.kafka.DockerKafkaEnvironment;
import io.specmesh.kafka.KafkaApiSpec;
import io.specmesh.kafka.KafkaEnvironment;
import io.specmesh.kafka.admin.SmAdminClient;
import io.specmesh.kafka.provision.Provisioner;
import io.specmesh.test.TestSpecLoader;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import picocli.CommandLine;
import simple.schema_demo._public.UserSignedUp;

@SuppressFBWarnings({"UW_UNCOND_WAIT", "WA_NOT_IN_LOOP"})
class StorageConsumptionFunctionalTest {

    private static final long RECORD_COUNT = 10_000;
    private static final String OWNER_USER = "simple.schema_demo";

    @RegisterExtension
    private static final KafkaEnvironment KAFKA_ENV =
            DockerKafkaEnvironment.builder()
                    .withSaslAuthentication(
                            "admin", "admin-secret", OWNER_USER, OWNER_USER + "-secret")
                    .withKafkaAcls()
                    .build();

    private static final KafkaApiSpec API_SPEC =
            TestSpecLoader.loadFromClassPath("simple_schema_demo-api.yaml");

    @Test
    void shouldGetStorageAndConsumptionMetrics() throws Exception {

        Provisioner.builder()
                .apiSpec(API_SPEC)
                .schemaPath("./src/test/resources")
                .adminClient(KAFKA_ENV.adminClient())
                .closeAdminClient(true)
                .schemaRegistryClient(KAFKA_ENV.srClient())
                .closeSchemaClient(true)
                .build()
                .provision()
                .check();

        try (Admin adminClient = KAFKA_ENV.adminClient()) {
            final var client = SmAdminClient.create(adminClient);

            shouldDoConsumptionStats(client);

            shouldDoConsumptionForStreams(client);

            shouldDoStorageStats();

            shouldExportStuff();
        }
    }

    private static void shouldExportStuff() throws Exception {
        final var command = Export.builder().build();
        final CommandLine.ParseResult parseResult =
                new CommandLine(command)
                        .parseArgs(
                                ("--bootstrap-server "
                                                + KAFKA_ENV.kafkaBootstrapServers()
                                                + " --domain-id simple:schema_demo"
                                                + " --username admin"
                                                + " --secret admin-secret")
                                        .split(" "));

        assertThat(parseResult.matchedArgs().size(), is(4));
        assertThat(command.call(), is(0));
        final var spec = command.state();
        assertThat(spec.id(), is("simple.schema_demo"));
        assertThat(
                spec.channels().keySet(),
                containsInAnyOrder(
                        "simple.schema_demo._public.user_info",
                        "simple.schema_demo._public.user_checkout",
                        "simple.schema_demo._public.user_signed_up"));
    }

    private void shouldDoConsumptionStats(final SmAdminClient client) throws Exception {
        final String userSignedUpTopic = seedTopicData();

        try (Consumer<Long, UserSignedUp> consumer =
                avroConsumer(userSignedUpTopic, OWNER_USER, "testGroup")) {
            // Initially should have no consumption:
            assertThat(client.groupsForTopicPrefix("simple"), is(empty()));

            final long consumedCount = consumeOneBatch(consumer);
            assertThat(consumedCount, is(not(RECORD_COUNT)));

            // Consumer group should not be registered, but not have offsets:
            final List<SmAdminClient.ConsumerGroup> preSync = client.groupsForTopicPrefix("simple");
            assertThat(preSync, hasSize(1));
            assertThat(preSync.get(0).members(), hasSize(1));
            assertThat(preSync.get(0).partitions(), is(empty()));
            assertThat(preSync.get(0).offsetTotal(), is(0L));

            // Sync offsets:
            consumer.commitSync();

            // VERIFY now check the positional info while the consumer is active
            final List<SmAdminClient.ConsumerGroup> postSync =
                    client.groupsForTopicPrefix("simple");
            System.out.println(postSync);
            assertThat(postSync, hasSize(1));
            assertThat(postSync.get(0).id(), is("simple.schema_demo:testGroup"));
            assertThat(postSync.get(0).members(), hasSize(1));
            assertThat(
                    postSync.get(0).members().get(0).clientId(),
                    is("simple.schema_demo.unique-service-id.consumer"));
            assertThat(
                    postSync.get(0).members().get(0).id(),
                    startsWith("simple.schema_demo.unique-service-id.consumer-"));
            assertThat(postSync.get(0).partitions(), hasSize(3));
            assertThat(postSync.get(0).offsetTotal(), is(consumedCount));

            checkConsumptionStats(consumedCount);
        }
    }

    @SuppressFBWarnings("NN_NAKED_NOTIFY")
    private void shouldDoConsumptionForStreams(final SmAdminClient client) {
        final Object lock = new Object();
        final AtomicInteger processedCount = new AtomicInteger();
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(
                        "simple.schema_demo._public.user_signed_up",
                        Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()))
                .foreach(
                        (k, v) -> {
                            if (processedCount.incrementAndGet() == 1_000) {
                                try {
                                    synchronized (lock) {
                                        lock.wait();
                                    }
                                } catch (InterruptedException e) {
                                    // Meh
                                }
                            }
                        });

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, API_SPEC.id() + ":streaming-app");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1L);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_ENV.kafkaBootstrapServers());
        props.put(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                KAFKA_ENV.kafkaBootstrapServers());
        props.putAll(Clients.clientSaslAuthProperties(OWNER_USER, OWNER_USER + "-secret"));

        try (KafkaStreams streamsApp = new KafkaStreams(builder.build(), props)) {
            streamsApp.start();

            try {
                assertThatEventually(streamsApp::state, is(KafkaStreams.State.RUNNING));
                assertThatEventually(processedCount::get, is(1_000));
                assertThatEventually(() -> client.groupsForTopicPrefix("simple"), hasSize(1));
                final List<SmAdminClient.ConsumerGroup> groups =
                        client.groupsForTopicPrefix("simple");
                assertThat(groups, hasSize(1));
                assertThat(groups.get(0).id(), is("simple.schema_demo:streaming-app"));
                assertThat(groups.get(0).members(), hasSize(1));
                assertThat(
                        groups.get(0).members().get(0).clientId(),
                        startsWith("simple.schema_demo:streaming-app-"));
                assertThat(
                        groups.get(0).members().get(0).id(),
                        startsWith("simple.schema_demo:streaming-app-"));
                assertThat(groups.get(0).partitions(), hasSize(1));
                assertThat(
                        groups.get(0).offsetTotal(),
                        is(
                                both(greaterThan(0L))
                                        .and(lessThanOrEqualTo((long) processedCount.get()))));
            } finally {
                synchronized (lock) {
                    lock.notifyAll();
                }
            }
        }
    }

    private static int consumeOneBatch(final Consumer<Long, UserSignedUp> consumer) {
        int count = 0;
        ConsumerRecords<?, ?> consumerRecords;
        do {
            consumerRecords = consumer.poll(Duration.ofSeconds(1));
            count += consumerRecords.count();
        } while (consumerRecords.isEmpty());
        return count;
    }

    // todo: can we get principal / account into this SpecMesh data?  Ideally we use SoecMesh for
    // this and NOT ccloud.
    // todo: does CCloud metrics API require use of RBAC? (Which we hate)
    // Todo: produce and consume data to CCLoud and check out metrics.

    private static void shouldDoStorageStats() throws Exception {
        final var command = Storage.builder().build();
        final CommandLine.ParseResult parseResult =
                new CommandLine(command)
                        .parseArgs(
                                ("--bootstrap-server "
                                                + KAFKA_ENV.kafkaBootstrapServers()
                                                + " --spec simple_schema_demo-api.yaml"
                                                + " --username admin"
                                                + " --secret admin-secret")
                                        .split(" "));

        assertThat(parseResult.matchedArgs().size(), is(4));
        assertThat(command.call(), is(0));
        final var storage = command.state();

        assertThat(storage.size(), is(API_SPEC.listDomainOwnedTopics().size()));

        final var volume =
                (Map<String, Long>) storage.get("simple.schema_demo._public.user_signed_up");

        assertThat(volume.get("offset-total"), is(RECORD_COUNT));
        assertThat(volume.get("storage-bytes"), is(1120000L));
    }

    private static void checkConsumptionStats(final long consumedCount) throws Exception {
        final var consumptionCommand = Consumption.builder().build();
        // Given:
        final CommandLine.ParseResult parseResult =
                new CommandLine(consumptionCommand)
                        .parseArgs(
                                ("--bootstrap-server "
                                                + KAFKA_ENV.kafkaBootstrapServers()
                                                + " --spec simple_schema_demo-api.yaml"
                                                + " --username admin"
                                                + " --secret admin-secret")
                                        .split(" "));

        assertThat(parseResult.matchedArgs().size(), is(4));

        assertThat(consumptionCommand.call(), is(0));

        final var consumptionMap = consumptionCommand.state();

        assertThat(consumptionMap.size(), is(1));
        assertThat(
                consumptionMap.keySet(), is(contains("simple.schema_demo._public.user_signed_up")));
        assertThat(consumptionMap.values().iterator().next().offsetTotal(), is(consumedCount));
    }

    private static String seedTopicData() {

        final var sentRecord = new UserSignedUp("joe blogs", "blogy@twasmail.com", 100);
        // write seed info
        final var userSignedUpTopic = "simple.schema_demo._public.user_signed_up";
        try (var producer = avroProducer(OWNER_USER)) {

            for (int i = 0; i < RECORD_COUNT; i++) {
                producer.send(new ProducerRecord<>(userSignedUpTopic, 1000L + i, sentRecord))
                        .get(60, TimeUnit.SECONDS);
            }
            producer.flush();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        return userSignedUpTopic;
    }

    private Consumer<Long, UserSignedUp> avroConsumer(
            final String topicName, final String userName, final String groupName) {
        return consumer(
                UserSignedUp.class, topicName, KafkaAvroDeserializer.class, userName, groupName);
    }

    private static <V> Consumer<Long, V> consumer(
            final Class<V> valueClass,
            final String topicName,
            final Class<?> valueDeserializer,
            final String userName,
            final String groupName) {

        final Map<String, Object> props =
                Clients.consumerProperties(
                        API_SPEC.id(),
                        "unique-service-id",
                        KAFKA_ENV.kafkaBootstrapServers(),
                        KAFKA_ENV.schemaRegistryServer(),
                        LongDeserializer.class,
                        valueDeserializer,
                        true,
                        Map.of(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false));

        props.putAll(Clients.clientSaslAuthProperties(userName, userName + "-secret"));
        props.put(CommonClientConfigs.GROUP_ID_CONFIG, userName + ":" + groupName);

        final KafkaConsumer<Long, V> consumer = Clients.consumer(Long.class, valueClass, props);
        consumer.subscribe(List.of(topicName));
        return consumer;
    }

    private static Producer<Long, UserSignedUp> avroProducer(final String user) {
        return producer(UserSignedUp.class, KafkaAvroSerializer.class, user, Map.of());
    }

    private static <V> Producer<Long, V> producer(
            final Class<V> valueClass,
            final Class<?> valueSerializer,
            final String userName,
            final Map<String, Object> additionalProps) {
        final Map<String, Object> props =
                producerProperties(
                        API_SPEC.id(),
                        UUID.randomUUID().toString(),
                        KAFKA_ENV.kafkaBootstrapServers(),
                        KAFKA_ENV.schemaRegistryServer(),
                        LongSerializer.class,
                        valueSerializer,
                        false,
                        additionalProps);

        props.putAll(Clients.clientSaslAuthProperties(userName, userName + "-secret"));

        return Clients.producer(Long.class, valueClass, props);
    }
}
