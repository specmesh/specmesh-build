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

import static io.specmesh.kafka.Clients.producerProperties;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

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
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import picocli.CommandLine;
import simple.schema_demo._public.user_signed_up_value.UserSignedUp;

class StorageConsumptionFunctionalTest {

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

        Provisioner.provision(
                true,
                false,
                false,
                API_SPEC,
                "./build/resources/test",
                KAFKA_ENV.adminClient(),
                Optional.of(KAFKA_ENV.srClient()));

        try (Admin adminClient = KAFKA_ENV.adminClient()) {
            final var client = SmAdminClient.create(adminClient);

            shouldDoConsumptionStats(client);

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
            final var consumerRecords = consumer.poll(Duration.ofSeconds(10));

            // VERIFY now check the positional info while the consumer is active
            final var stats = client.groupsForTopicPrefix("simple");
            System.out.println(stats);
            assertThat(stats.get(0).offsetTotal(), is(10000L));

            checkConsumptionStats();
        }
    }

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

        assertThat(volume.get("storage"), is(1120000L));
        assertThat(volume.get("offset-total"), is(10000L));
    }

    private static void checkConsumptionStats() throws Exception {
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
        assertThat(consumptionMap.values().iterator().next().offsetTotal(), is(10000L));
    }

    private static String seedTopicData()
            throws InterruptedException, ExecutionException, TimeoutException {

        final var sentRecord = new UserSignedUp("joe blogs", "blogy@twasmail.com", 100);
        // write seed info
        final var userSignedUpTopic = "simple.schema_demo._public.user_signed_up";
        try (var producer = avroProducer(OWNER_USER)) {

            for (int i = 0; i < 10000; i++) {
                producer.send(new ProducerRecord<>(userSignedUpTopic, 1000L + i, sentRecord))
                        .get(60, TimeUnit.SECONDS);
            }
            producer.flush();
        }
        return userSignedUpTopic;
    }

    private Consumer<Long, UserSignedUp> avroConsumer(
            final String topicName, final String userName, final String groupName) {
        return consumer(
                UserSignedUp.class,
                topicName,
                KafkaAvroDeserializer.class,
                userName,
                Map.of("group.id", groupName));
    }

    private static <V> Consumer<Long, V> consumer(
            final Class<V> valueClass,
            final String topicName,
            final Class<?> valueDeserializer,
            final String userName,
            final Map<String, Object> additionalProps) {

        final Map<String, Object> props =
                Clients.consumerProperties(
                        API_SPEC.id(),
                        UUID.randomUUID().toString(),
                        KAFKA_ENV.kafkaBootstrapServers(),
                        KAFKA_ENV.schemeRegistryServer(),
                        LongDeserializer.class,
                        valueDeserializer,
                        true,
                        additionalProps);

        props.putAll(Clients.clientSaslAuthProperties(userName, userName + "-secret"));
        props.put(CommonClientConfigs.GROUP_ID_CONFIG, userName);

        final KafkaConsumer<Long, V> consumer = Clients.consumer(Long.class, valueClass, props);
        consumer.subscribe(List.of(topicName));
        consumer.poll(Duration.ofSeconds(1));
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
                        KAFKA_ENV.schemeRegistryServer(),
                        LongSerializer.class,
                        valueSerializer,
                        false,
                        additionalProps);

        props.putAll(Clients.clientSaslAuthProperties(userName, userName + "-secret"));

        return Clients.producer(Long.class, valueClass, props);
    }
}
