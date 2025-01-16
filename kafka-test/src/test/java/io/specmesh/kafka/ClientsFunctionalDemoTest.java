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

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY;
import static java.util.stream.Collectors.toList;
import static org.apache.kafka.common.acl.AclOperation.ALL;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.common.resource.PatternType.LITERAL;
import static org.apache.kafka.common.resource.PatternType.PREFIXED;
import static org.apache.kafka.common.resource.Resource.CLUSTER_NAME;
import static org.apache.kafka.common.resource.ResourceType.CLUSTER;
import static org.apache.kafka.common.resource.ResourceType.GROUP;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import io.specmesh.kafka.Clients.KStreamsProperties;
import io.specmesh.kafka.provision.Provisioner;
import io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo;
import io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfoEnriched.UserInfoEnriched;
import io.specmesh.test.TestSpecLoader;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import simple.schema_demo.UserSignedUp;
import simple.schema_demo.UserSignedUpPojo;

@SuppressWarnings("unchecked")
class ClientsFunctionalDemoTest {

    private static final KafkaApiSpec API_SPEC =
            TestSpecLoader.loadFromClassPath("kafka_test-simple_schema_demo-api.yaml");

    private static final String OWNER_USER = API_SPEC.id();
    private static final String DIFFERENT_USER = "different-user";

    @RegisterExtension
    private static final KafkaEnvironment KAFKA_ENV =
            DockerKafkaEnvironment.builder()
                    .withSaslAuthentication(
                            "admin",
                            "admin-secret",
                            OWNER_USER,
                            OWNER_USER + "-secret",
                            DIFFERENT_USER,
                            DIFFERENT_USER + "-secret")
                    .withKafkaAcls(aclsForOtherDomain())
                    .build();

    private SchemaRegistryClient schemaRegistryClient;

    @BeforeAll
    public static void provision() {
        Provisioner.builder()
                .apiSpec(API_SPEC)
                .schemaPath("./build/resources/test")
                .adminClient(KAFKA_ENV.adminClient())
                .closeAdminClient(true)
                .schemaRegistryClient(KAFKA_ENV.srClient())
                .closeSchemaClient(true)
                .build()
                .provision()
                .check();
    }

    @BeforeEach
    public void setUp() {
        schemaRegistryClient = KAFKA_ENV.srClient();
    }

    @Test
    void shouldProduceAndConsumeUsingAvroWithReflection() {
        // Given:
        final var userSignedUpTopic = topicName("_public.user_signed_up_pojo");
        final var sentRecord = new UserSignedUpPojo("joe blogs", "blogy@twasmail.com", 100);

        try (Consumer<Long, UserSignedUpPojo> consumer =
                        avroConsumer(
                                UserSignedUpPojo.class,
                                userSignedUpTopic,
                                OWNER_USER,
                                RecordNameStrategy.class,
                                true);
                Producer<Long, UserSignedUpPojo> producer =
                        avroProducer(
                                UserSignedUpPojo.class,
                                OWNER_USER,
                                RecordNameStrategy.class,
                                true)) {

            // When:
            producer.send(new ProducerRecord<>(userSignedUpTopic, 1000L, sentRecord));
            producer.flush();

            // Then:
            assertThat(values(consumer), contains(sentRecord));
        }
    }

    @Test
    void shouldProduceAndConsumeUsingAvroWithOutReflection() {
        // Given:
        final var userSignedUpTopic = topicName("_public.user_signed_up");
        final var sentRecord = new UserSignedUp("joe blogs", "blogy@twasmail.com", 100);

        try (Consumer<Long, UserSignedUp> consumer =
                        avroConsumer(
                                UserSignedUp.class,
                                userSignedUpTopic,
                                OWNER_USER,
                                RecordNameStrategy.class);
                Producer<Long, UserSignedUp> producer =
                        avroProducer(UserSignedUp.class, OWNER_USER, RecordNameStrategy.class)) {

            // When:
            producer.send(new ProducerRecord<>(userSignedUpTopic, 1000L, sentRecord));
            producer.flush();

            // Then:
            assertThat(values(consumer), contains(sentRecord));
        }
    }

    @Test
    void shouldProduceAndConsumeProto() {
        // Given:
        final var userInfoTopic = topicName("_public.user_info");
        final var userSam =
                UserInfo.newBuilder()
                        .setFullName("sam fteex")
                        .setEmail("hello-sam@bahamas.island")
                        .setAge(52)
                        .build();

        try (Consumer<Long, UserInfo> consumer =
                        protoConsumer(Long.class, UserInfo.class, userInfoTopic);
                Producer<Long, UserInfo> producer = protoProducer()) {

            // When:
            producer.send(new ProducerRecord<>(userInfoTopic, 1000L, userSam));
            producer.flush();

            // Then:
            assertThat(values(consumer), contains(userSam));
        }
    }

    @SuppressWarnings("unused")
    @Test
    void shouldStreamStuffUsingProto() {
        // Given:
        final String userInfoTopic = topicName("_public.user_info");
        final String userInfoEnrichedTopic = topicName("_public.user_info_enriched");
        final String repartitionByAge =
                topicName("_private.client-func-demo-rekey.by-age-repartition");
        final String storeByAge = topicName("_private.client-func-demo-store.age-changelog");

        final long key = 1000L;
        final UserInfo userSam =
                UserInfo.newBuilder()
                        .setFullName("sam fteex")
                        .setEmail("hello-sam@bahamas.island")
                        .setAge(52)
                        .build();

        try (Consumer<Long, UserInfoEnriched> enhancedConsumer =
                        protoConsumer(Long.class, UserInfoEnriched.class, userInfoEnrichedTopic);
                Consumer<Integer, Integer> repartitionConsumer =
                        protoConsumer(Integer.class, Integer.class, repartitionByAge);
                Consumer<Long, Integer> storeConsumer =
                        protoConsumer(Long.class, Integer.class, storeByAge);
                KafkaStreams streamsApp = streamsApp(userInfoTopic, userInfoEnrichedTopic);
                Producer<Long, UserInfo> producer = protoProducer()) {

            // When:
            producer.send(new ProducerRecord<>(userInfoTopic, key, userSam));
            producer.flush();

            // Then:
            final var expectedEnriched =
                    UserInfoEnriched.newBuilder()
                            .setAddress("hiding in the bahamas")
                            .setAge(userSam.getAge())
                            .setEmail(userSam.getEmail())
                            .build();

            assertThat(entries(enhancedConsumer, 1), contains(Map.entry(key, expectedEnriched)));
            assertThat(entries(repartitionConsumer, 1), contains(Map.entry(userSam.getAge(), 1)));
            assertThat(entries(storeConsumer, 1), contains(Map.entry(key, (userSam.getAge()))));
        }
    }

    @Test
    void shouldFailToProduceWithDifferentUser() {
        // Given:
        final var userSignedUpTopic = topicName("_public.user_signed_up");
        final var sentRecord = new UserSignedUp("joe blogs", "blogy@twasmail.com", 100);

        try (Producer<Long, UserSignedUp> producer =
                avroProducer(UserSignedUp.class, DIFFERENT_USER, RecordNameStrategy.class)) {

            // When:
            final Future<RecordMetadata> f =
                    producer.send(new ProducerRecord<>(userSignedUpTopic, 1000L, sentRecord));

            // Then:
            final Exception e = assertThrows(ExecutionException.class, f::get);
            assertThat(e.getCause(), is(instanceOf(TopicAuthorizationException.class)));
        }
    }

    @Nested
    class NestedTest {

        // Run one test in a nested class to test env works with such nesting...

        @Test
        void shouldConsumeWithDifferentUser() {
            // Given:
            final var userSignedUpTopic = topicName("_public.user_signed_up");
            final var sentRecord = new UserSignedUp("joe blogs", "blogy@twasmail.com", 100);

            try (Consumer<Long, UserSignedUp> consumer =
                            avroConsumer(
                                    UserSignedUp.class,
                                    userSignedUpTopic,
                                    DIFFERENT_USER,
                                    RecordNameStrategy.class);
                    Producer<Long, UserSignedUp> producer =
                            avroProducer(
                                    UserSignedUp.class, OWNER_USER, RecordNameStrategy.class)) {

                // When:
                producer.send(new ProducerRecord<>(userSignedUpTopic, 1000L, sentRecord));
                producer.flush();

                // Then:
                final var values = values(consumer);
                assertThat(values, contains(sentRecord));
            }
        }
    }

    @Test
    void shouldProduceAndConsumeUsingAvroTopicRecordNameStrategy() {
        // Given:
        final String topicName = topicName("_public.user_signed_up_2");
        final var sentRecord = new UserSignedUp("joe blogs", "blogy@twasmail.com", 100);

        try (Consumer<Long, UserSignedUp> consumer =
                        avroConsumer(
                                UserSignedUp.class,
                                topicName,
                                OWNER_USER,
                                TopicRecordNameStrategy.class);
                Producer<Long, UserSignedUp> producer =
                        avroProducer(
                                UserSignedUp.class, OWNER_USER, TopicRecordNameStrategy.class)) {

            // When:
            producer.send(new ProducerRecord<>(topicName, 1000L, sentRecord));
            producer.flush();

            // Then:
            assertThat(values(consumer), contains(sentRecord));
        }
    }

    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
    private static <K> Clients.ConsumerBuilder<K, Void> consumerBuilder(
            final Class<K> keyClass, final String userName, final Map<String, ?> additionalProps) {

        return Clients.builder(
                        API_SPEC.id(),
                        UUID.randomUUID().toString(),
                        KAFKA_ENV.kafkaBootstrapServers(),
                        KAFKA_ENV.schemaRegistryServer())
                .withProps(additionalProps)
                .withProps(Clients.clientSaslAuthProperties(userName, userName + "-secret"))
                .withProp(CommonClientConfigs.GROUP_ID_CONFIG, userName + "-" + UUID.randomUUID())
                .consumer()
                .withKeyType(keyClass)
                .withAutoOffsetReset(false);
    }

    @SuppressWarnings("resource")
    private static <K, V> Consumer<K, V> protoConsumer(
            final Class<K> keyClass, final Class<V> valueClass, final String topicName) {

        final Map<String, Class<V>> additional =
                Map.of(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, valueClass);

        final Consumer<K, V> consumer =
                consumerBuilder(keyClass, OWNER_USER, additional).withValueType(valueClass).build();

        return subscribe(topicName, consumer);
    }

    private <V> Consumer<Long, V> avroConsumer(
            final Class<V> valueType,
            final String topicName,
            final String userName,
            final Class<? extends SubjectNameStrategy> namingStrategy) {
        return avroConsumer(valueType, topicName, userName, namingStrategy, false);
    }

    @SuppressWarnings("resource")
    private <V> Consumer<Long, V> avroConsumer(
            final Class<V> valueType,
            final String topicName,
            final String userName,
            final Class<? extends SubjectNameStrategy> namingStrategy,
            final boolean useReflection) {

        final Map<String, Object> additionalProps =
                new HashMap<>(Map.of(VALUE_SUBJECT_NAME_STRATEGY, namingStrategy.getName()));

        if (useReflection) {
            // Turn on use of reflect
            additionalProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REFLECTION_CONFIG, true);
        } else {
            // Have deserializer return generated type, not GenericRecord.
            additionalProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        }

        final Consumer<Long, V> consumer =
                consumerBuilder(Long.class, userName, additionalProps)
                        .withValueDeserializerType(KafkaAvroDeserializer.class, valueType)
                        .build();

        return subscribe(topicName, consumer);
    }

    private static <K, V> Consumer<K, V> subscribe(
            final String topicName, final Consumer<K, V> consumer) {
        consumer.subscribe(List.of(topicName));
        consumer.poll(Duration.ofMillis(250));
        return consumer;
    }

    @SuppressWarnings("rawtypes")
    private static <V> Producer<Long, V> producer(
            final Class<V> valueType,
            final Class<? extends Serializer> valueSerializer,
            final String userName,
            final Map<String, ?> additionalProps) {

        return Clients.builder(
                        API_SPEC.id(),
                        UUID.randomUUID().toString(),
                        KAFKA_ENV.kafkaBootstrapServers(),
                        KAFKA_ENV.schemaRegistryServer())
                .withProps(additionalProps)
                .withProps(Clients.clientSaslAuthProperties(userName, userName + "-secret"))
                .producer()
                .withAcks(false)
                .withKeyType(Long.class)
                .withValueSerializerType(valueSerializer, valueType)
                .build();
    }

    private static Producer<Long, UserInfo> protoProducer() {
        return producer(UserInfo.class, KafkaProtobufSerializer.class, OWNER_USER, Map.of());
    }

    private static <V> Producer<Long, V> avroProducer(
            final Class<V> valueType,
            final String user,
            final Class<? extends SubjectNameStrategy> namingStrategy) {
        return avroProducer(valueType, user, namingStrategy, false);
    }

    private static <V> Producer<Long, V> avroProducer(
            final Class<V> valueType,
            final String user,
            final Class<? extends SubjectNameStrategy> namingStrategy,
            final boolean useReflection) {

        final Map<String, Object> additionalProps =
                new HashMap<>(Map.of(VALUE_SUBJECT_NAME_STRATEGY, namingStrategy.getName()));

        if (useReflection) {
            // Turn on use of reflect.
            // However, this will generate the wrong schema,
            // which it won't find in the schema registry.
            additionalProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REFLECTION_CONFIG, true);

            // Hence, need to also tell it to just use the latest schema,
            // i.e. the one SpecMesh registered
            additionalProps.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, true);
        }

        return producer(valueType, KafkaAvroSerializer.class, user, additionalProps);
    }

    @SuppressWarnings("SameParameterValue")
    private static <K, V> List<Map.Entry<K, V>> entries(
            final Consumer<K, V> consumer, final int expectedCount) {
        final Instant timeout = Instant.now().plus(Duration.ofSeconds(10));
        final List<Map.Entry<K, V>> entries = new ArrayList<>();
        while (entries.size() < expectedCount && Instant.now().isBefore(timeout)) {
            consumer.poll(Duration.ofMillis(100))
                    .forEach(record -> entries.add(Map.entry(record.key(), record.value())));
        }

        if (Instant.now().isBefore(timeout)) {
            // Poll to detect unexpected messages:
            consumer.poll(Duration.ofMillis(100))
                    .forEach(record -> entries.add(Map.entry(record.key(), record.value())));
        }

        return entries;
    }

    private static <V> List<V> values(final Consumer<?, V> consumer) {
        return entries(consumer, 1).stream().map(Map.Entry::getValue).collect(toList());
    }

    private static String topicName(final String topicSuffix) {
        return API_SPEC.listDomainOwnedTopics().stream()
                .filter(topic -> topic.name().endsWith(topicSuffix))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Topic " + topicSuffix + " not found"))
                .name();
    }

    private KafkaStreams streamsApp(
            final String userInfoTopic, final String userInfoEnrichedTopic) {

        final KStreamsProperties<Long, UserInfo> props =
                Clients.builder(
                                API_SPEC.id(),
                                "client-func-demo",
                                KAFKA_ENV.kafkaBootstrapServers(),
                                KAFKA_ENV.schemaRegistryServer())
                        .withProps(
                                Clients.clientSaslAuthProperties(
                                        OWNER_USER, OWNER_USER + "-secret"))
                        .withProp(
                                KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE,
                                UserInfo.class)
                        .withProp(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1L)
                        .kstreams()
                        .withKeySerdeType(Serdes.LongSerde.class, Long.class)
                        .withValueSerdeType(KafkaProtobufSerde.class, UserInfo.class)
                        .withAcks(false)
                        .buildProperties();

        final KafkaProtobufSerde<UserInfoEnriched> enhancedSerde =
                new KafkaProtobufSerde<>(schemaRegistryClient, UserInfoEnriched.class);

        final var builder = new StreamsBuilder();

        final String storeName = "store.age";
        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                                Stores.inMemoryKeyValueStore(storeName),
                                Serdes.Long(),
                                Serdes.Integer())
                        .withLoggingEnabled(Map.of()));

        final KStream<Long, UserInfo> users = builder.stream(userInfoTopic, Consumed.as("ingest"));

        users.mapValues(
                        userInfo ->
                                UserInfoEnriched.newBuilder()
                                        .setAddress("hiding in the bahamas")
                                        .setAge(userInfo.getAge())
                                        .setEmail(userInfo.getEmail())
                                        .build(),
                        Named.as("enhance"))
                .to(
                        userInfoEnrichedTopic,
                        Produced.<Long, UserInfoEnriched>as("emit-enhanced")
                                .withValueSerde(enhancedSerde));

        users.process(
                new ProcessorSupplier<>() {
                    @Override
                    public Processor<Long, UserInfo, Object, Object> get() {
                        return new Processor<>() {

                            private KeyValueStore<Long, Integer> store;

                            @Override
                            public void init(final ProcessorContext<Object, Object> context) {
                                this.store = context.getStateStore(storeName);
                            }

                            @Override
                            public void process(final Record<Long, UserInfo> record) {
                                store.put(record.key(), record.value().getAge());
                            }
                        };
                    }
                },
                Named.as("store"),
                storeName);

        users.map((k, v) -> new KeyValue<>(v.getAge(), 1), Named.as("select-key"))
                .groupByKey(
                        Grouped.<Integer, Integer>as("rekey.by-age")
                                .withKeySerde(Serdes.Integer())
                                .withValueSerde(Serdes.Integer()))
                .count(Named.as("count-by-age"));

        final var streams = new KafkaStreams(builder.build(), props.asProperties());
        streams.start();
        return streams;
    }

    private static Set<AclBinding> aclsForOtherDomain() {
        final String principal = "User:" + DIFFERENT_USER;
        return Set.of(
                new AclBinding(
                        new ResourcePattern(CLUSTER, CLUSTER_NAME, LITERAL),
                        new AccessControlEntry(principal, "*", ALL, ALLOW)),
                new AclBinding(
                        new ResourcePattern(GROUP, DIFFERENT_USER, PREFIXED),
                        new AccessControlEntry(principal, "*", ALL, ALLOW)));
    }
}
