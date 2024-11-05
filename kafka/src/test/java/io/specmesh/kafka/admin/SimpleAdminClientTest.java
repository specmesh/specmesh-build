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

package io.specmesh.kafka.admin;

import static io.specmesh.kafka.Clients.producerProperties;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.specmesh.kafka.Clients;
import io.specmesh.kafka.DockerKafkaEnvironment;
import io.specmesh.kafka.KafkaApiSpec;
import io.specmesh.kafka.KafkaEnvironment;
import io.specmesh.kafka.provision.Provisioner;
import io.specmesh.kafka.provision.Status;
import io.specmesh.test.TestSpecLoader;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
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
import simple.schema_demo.UserSignedUp;

class SimpleAdminClientTest {

    private static final String OWNER_USER = "simple.schema_demo";

    @RegisterExtension
    private static final KafkaEnvironment KAFKA_ENV =
            DockerKafkaEnvironment.builder()
                    .withSaslAuthentication(
                            "admin", "admin-secret", OWNER_USER, OWNER_USER + "-secret")
                    .withKafkaAcls(List.of())
                    .build();

    private static final KafkaApiSpec API_SPEC =
            TestSpecLoader.loadFromClassPath("clientapi-functional-test-api.yaml");

    @Test
    void shouldRecordStats() throws Exception {

        final var userSignedUpTopic = topicName("_public.user_signed_up");
        final var sentRecord = new UserSignedUp("joe blogs", "blogy@twasmail.com", 100);

        try (Admin adminClient = KAFKA_ENV.adminClient()) {
            final var client = SmAdminClient.create(adminClient);

            final Status status =
                    Provisioner.builder()
                            .apiSpec(API_SPEC)
                            .schemaPath("./src/test/resources")
                            .adminClient(adminClient)
                            .schemaRegistryClient(KAFKA_ENV.srClient())
                            .closeSchemaClient(true)
                            .build()
                            .provision();

            status.check();

            // write seed info
            try (var producer = avroProducer(OWNER_USER)) {

                for (int i = 0; i < 10000; i++) {
                    producer.send(new ProducerRecord<>(userSignedUpTopic, 1000L + i, sentRecord))
                            .get(60, TimeUnit.SECONDS);
                }
                producer.flush();
            }

            try (Consumer<Long, UserSignedUp> consumer =
                    avroConsumer(userSignedUpTopic, OWNER_USER, "testGroup")) {
                final int count = consumer.poll(Duration.ofSeconds(1)).count();
                consumer.commitSync();

                // VERIFY now check the positional info while the consumer is active
                final var stats = client.groupsForTopicPrefix("simple");
                System.out.println(stats);
                assertThat(stats.get(0).offsetTotal(), is((long) count));
            }

            final var bytesUsingLogDirs = client.topicVolumeUsingLogDirs(userSignedUpTopic);
            assertThat(bytesUsingLogDirs, is(1120000L));
            final var offsets = client.topicVolumeOffsets(userSignedUpTopic);

            assertThat(offsets, is(10000L));
        }
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
                        KAFKA_ENV.schemaRegistryServer(),
                        LongDeserializer.class,
                        valueDeserializer,
                        true,
                        additionalProps);

        props.putAll(Clients.clientSaslAuthProperties(userName, userName + "-secret"));
        props.put(CommonClientConfigs.GROUP_ID_CONFIG, userName);

        final KafkaConsumer<Long, V> consumer = Clients.consumer(Long.class, valueClass, props);
        consumer.subscribe(List.of(topicName));
        return consumer;
    }

    private static Producer<Long, UserSignedUp> avroProducer(final String user) {
        return producer(UserSignedUp.class, KafkaAvroSerializer.class, user, Map.of());
    }

    private static String topicName(final String topicSuffix) {
        return API_SPEC.listDomainOwnedTopics().stream()
                .filter(topic -> topic.name().endsWith(topicSuffix))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Topic " + topicSuffix + " not found"))
                .name();
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
