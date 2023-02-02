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

import static io.specmesh.kafka.Clients.producerProperties;
import static java.util.stream.Collectors.toList;
import static org.apache.kafka.streams.kstream.Produced.with;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import io.specmesh.apiparser.AsyncApiParser;
import io.specmesh.apiparser.model.ApiSpec;
import io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo;
import io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfoEnriched.UserInfoEnriched;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;
import org.apache.commons.collections.MapUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import simple.schema_demo._public.user_signed_up_value.UserSignedUp;

class ClientsFunctionalDemoTest {
    private static final KafkaApiSpec apiSpec = new KafkaApiSpec(getAPISpecFromResource());

    private static final String ADMIN_USER = "admin";
    private static final String ADMIN_PASSWORD = "admin-secret";

    private static final String OWNER_USER = "simple.schema_demo";
    private static final String OWNER_PASSWORD = "simple.schema_demo-secret";

    private static final String DIFFERENT_USER = "different-user";
    private static final String DIFFERENT_PASSWORD = "something no one will guess... ever!";

    @RegisterExtension
    private static final KafkaEnvironment KAFKA_ENV =
            DockerKafkaEnvironment.builder()
                    .withSaslAuthentication(
                            ADMIN_USER,
                            ADMIN_PASSWORD,
                            OWNER_USER,
                            OWNER_PASSWORD,
                            DIFFERENT_USER,
                            DIFFERENT_PASSWORD)
                    .withKafkaAcls()
                    .build();

    private SchemaRegistryClient schemaRegistryClient;

    @BeforeAll
    public static void provision() {
        try (Admin adminClient = adminClient()) {
            final SchemaRegistryClient schemaRegistryClient =
                    new CachedSchemaRegistryClient(KAFKA_ENV.schemeRegistryServer(), 5);
            Provisioner.provision(
                    apiSpec, "./build/resources/test", adminClient, schemaRegistryClient);
        }
    }

    @BeforeEach
    public void setUp() {
        schemaRegistryClient = new CachedSchemaRegistryClient(KAFKA_ENV.schemeRegistryServer(), 5);
    }

    @Test
    void shouldProduceAndConsumeUsingAvro() throws Exception {
        // Given:
        final var userSignedUpTopic = topicName("_public.user_signed_up");
        final var sentRecord = new UserSignedUp("joe blogs", "blogy@twasmail.com", 100);

        try (Consumer<Long, UserSignedUp> consumer =
                        avroConsumer(UserSignedUp.class, userSignedUpTopic);
                Producer<Long, UserSignedUp> producer = avroProducer(UserSignedUp.class)) {

            // When:
            producer.send(new ProducerRecord<>(userSignedUpTopic, 1000L, sentRecord))
                    .get(60, TimeUnit.SECONDS);

            // Then:
            assertThat(values(consumer), contains(sentRecord));
        }
    }

    @Test
    void shouldProduceAndConsumeProto() throws Exception {
        // Given:
        final var userInfoTopic = topicName("_public.user_info");
        final var userSam =
                UserInfo.newBuilder()
                        .setFullName("sam fteex")
                        .setEmail("hello-sam@bahamas.island")
                        .setAge(52)
                        .build();

        try (Consumer<Long, UserInfo> consumer = protoConsumer(UserInfo.class, userInfoTopic);
                Producer<Long, UserInfo> producer = protoProducer(UserInfo.class)) {

            // When:
            producer.send(new ProducerRecord<>(userInfoTopic, 1000L, userSam))
                    .get(60, TimeUnit.SECONDS);

            // Then:
            assertThat(values(consumer), contains(userSam));
        }
    }

    @SuppressWarnings("unused")
    @Test
    void shouldStreamStuffUsingProto() throws Exception {
        // Given:
        final var userInfoTopic = topicName("_public.user_info");
        final var userInfoEnrichedTopic = topicName("_public.user_info_enriched");
        final var userSam =
                UserInfo.newBuilder()
                        .setFullName("sam fteex")
                        .setEmail("hello-sam@bahamas.island")
                        .setAge(52)
                        .build();
        final var expectedEnriched =
                UserInfoEnriched.newBuilder()
                        .setAddress("hiding in the bahamas")
                        .setAge(userSam.getAge())
                        .setEmail(userSam.getEmail())
                        .build();

        try (Consumer<Long, UserInfoEnriched> consumer =
                        protoConsumer(UserInfoEnriched.class, userInfoEnrichedTopic);
                AutoCloseable streamsApp = streamsApp(userInfoTopic, userInfoEnrichedTopic);
                Producer<Long, UserInfo> producer = protoProducer(UserInfo.class)) {

            // When:
            producer.send(new ProducerRecord<>(userInfoTopic, 1000L, userSam))
                    .get(60, TimeUnit.SECONDS);

            // Then:
            assertThat(values(consumer), contains(expectedEnriched));
        }
    }

    @Test
    void shouldFailToProduceWithDifferentUser() {
        // Given:
        final var userSignedUpTopic = topicName("_public.user_signed_up");
        final var sentRecord = new UserSignedUp("joe blogs", "blogy@twasmail.com", 100);

        final Map<String, Object> differentUser =
                Provisioner.clientSaslAuthProperties(DIFFERENT_USER, DIFFERENT_PASSWORD);

        try (Producer<Long, UserSignedUp> producer =
                avroProducer(UserSignedUp.class, differentUser)) {

            // When:
            final Future<RecordMetadata> f =
                    producer.send(new ProducerRecord<>(userSignedUpTopic, 1000L, sentRecord));

            // Then:
            final Exception e = assertThrows(ExecutionException.class, f::get);
            assertThat(e.getCause(), is(instanceOf(TopicAuthorizationException.class)));
        }
    }

    @Test
    void shouldConsumeWithDifferentUser() throws Exception {
        // Given:
        final var userSignedUpTopic = topicName("_public.user_signed_up");
        final var sentRecord = new UserSignedUp("joe blogs", "blogy@twasmail.com", 100);

        final Map<String, Object> differentUser =
                Provisioner.clientSaslAuthProperties(DIFFERENT_USER, DIFFERENT_PASSWORD);

        try (Consumer<Long, UserSignedUp> consumer =
                        avroConsumer(UserSignedUp.class, userSignedUpTopic, differentUser);
                Producer<Long, UserSignedUp> producer = avroProducer(UserSignedUp.class)) {

            // When:
            producer.send(new ProducerRecord<>(userSignedUpTopic, 1000L, sentRecord))
                    .get(60, TimeUnit.SECONDS);

            // Then:
            assertThat(values(consumer), contains(sentRecord));
        }
    }

    private static ApiSpec getAPISpecFromResource() {
        try {
            return new AsyncApiParser()
                    .loadResource(
                            ClientsFunctionalDemoTest.class
                                    .getClassLoader()
                                    .getResourceAsStream("simple_schema_demo-api.yaml"));
        } catch (IOException e) {
            throw new RuntimeException("Failed to load api spec", e);
        }
    }

    private static AdminClient adminClient() {
        final Map<String, Object> properties =
                new HashMap<>(Provisioner.clientSaslAuthProperties(ADMIN_USER, ADMIN_PASSWORD));
        properties.put(AdminClientConfig.CLIENT_ID_CONFIG, apiSpec.id());
        properties.put(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_ENV.kafkaBootstrapServers());
        properties.putAll(Provisioner.clientSaslAuthProperties(ADMIN_USER, ADMIN_PASSWORD));
        return AdminClient.create(properties);
    }

    private static <V> Consumer<Long, V> consumer(
            final Class<V> valueClass,
            final String topicName,
            final Class<?> valueDeserializer,
            final Map<String, Object> additionalProps) {
        final Map<String, Object> props =
                Clients.consumerProperties(
                        apiSpec.id(),
                        UUID.randomUUID().toString(),
                        KAFKA_ENV.kafkaBootstrapServers(),
                        KAFKA_ENV.schemeRegistryServer(),
                        LongDeserializer.class,
                        valueDeserializer,
                        false,
                        additionalProps);

        final KafkaConsumer<Long, V> consumer = Clients.consumer(Long.class, valueClass, props);
        consumer.subscribe(List.of(topicName));
        consumer.poll(Duration.ofSeconds(1));
        return consumer;
    }

    private static <V> Consumer<Long, V> protoConsumer(
            final Class<V> valueClass, final String topicName) {
        final Map<String, Object> props =
                new HashMap<>(Provisioner.clientSaslAuthProperties(OWNER_USER, OWNER_PASSWORD));
        props.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, valueClass);

        return consumer(valueClass, topicName, KafkaProtobufDeserializer.class, props);
    }

    private <V> Consumer<Long, V> avroConsumer(
            final Class<V> valueClass,
            final String topicName,
            final Map<String, Object> additionalProps) {
        return consumer(valueClass, topicName, KafkaAvroDeserializer.class, additionalProps);
    }

    private <V> Consumer<Long, V> avroConsumer(final Class<V> valueClass, final String topicName) {
        return avroConsumer(
                valueClass,
                topicName,
                Provisioner.clientSaslAuthProperties(OWNER_USER, OWNER_PASSWORD));
    }

    private static <V> Producer<Long, V> producer(
            final Class<V> valueClass,
            final Class<?> valueSerializer,
            final Map<String, Object> additionalProps) {
        final Map<String, Object> props =
                producerProperties(
                        apiSpec.id(),
                        UUID.randomUUID().toString(),
                        KAFKA_ENV.kafkaBootstrapServers(),
                        KAFKA_ENV.schemeRegistryServer(),
                        LongSerializer.class,
                        valueSerializer,
                        false,
                        additionalProps);

        return Clients.producer(Long.class, valueClass, props);
    }

    private static <V> Producer<Long, V> protoProducer(final Class<V> valueClass) {
        return producer(
                valueClass,
                KafkaProtobufSerializer.class,
                Provisioner.clientSaslAuthProperties(OWNER_USER, OWNER_PASSWORD));
    }

    private static <V> Producer<Long, V> avroProducer(
            final Class<V> valueClass, final Map<String, Object> additionalProps) {
        return producer(valueClass, KafkaAvroSerializer.class, additionalProps);
    }

    private static <V> Producer<Long, V> avroProducer(final Class<V> valueClass) {
        return avroProducer(
                valueClass, Provisioner.clientSaslAuthProperties(OWNER_USER, OWNER_PASSWORD));
    }

    private static <V> List<V> values(final Consumer<Long, V> consumer) {
        final ConsumerRecords<Long, V> consumerRecords = consumer.poll(Duration.ofSeconds(10));
        return StreamSupport.stream(consumerRecords.spliterator(), false)
                .map(ConsumerRecord::value)
                .collect(toList());
    }

    private static String topicName(final String topicSuffix) {
        return apiSpec.listDomainOwnedTopics().stream()
                .filter(topic -> topic.name().endsWith(topicSuffix))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Topic " + topicSuffix + " not found"))
                .name();
    }

    private AutoCloseable streamsApp(
            final String userInfoTopic, final String userInfoEnrichedTopic) {
        final var props =
                Clients.kstreamsProperties(
                        apiSpec.id(),
                        "streams-appid-service-thing",
                        KAFKA_ENV.kafkaBootstrapServers(),
                        KAFKA_ENV.schemeRegistryServer(),
                        Serdes.LongSerde.class,
                        KafkaProtobufSerde.class,
                        false,
                        Provisioner.clientSaslAuthProperties(OWNER_USER, OWNER_PASSWORD),
                        Map.of(
                                KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE,
                                UserInfo.class));

        final var builder = new StreamsBuilder();

        builder.<Long, UserInfo>stream(userInfoTopic)
                .mapValues(
                        userInfo ->
                                UserInfoEnriched.newBuilder()
                                        .setAddress("hiding in the bahamas")
                                        .setAge(userInfo.getAge())
                                        .setEmail(userInfo.getEmail())
                                        .build())
                .to(
                        userInfoEnrichedTopic,
                        with(
                                Serdes.Long(),
                                new KafkaProtobufSerde<>(
                                        schemaRegistryClient, UserInfoEnriched.class)));

        final var streams = new KafkaStreams(builder.build(), MapUtils.toProperties(props));
        streams.start();
        return streams;
    }
}
