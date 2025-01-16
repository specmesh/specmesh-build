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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.core.Is.is;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.specmesh.kafka.Clients.ConsumerProperties;
import io.specmesh.kafka.Clients.KStreamsProperties;
import io.specmesh.kafka.Clients.ProducerProperties;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.apache.kafka.common.serialization.VoidSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("removal")
class ClientsTest {

    private static final String DOMAIN_ID = "some.domain";
    private static final String SERVICE_ID = "some-service";
    private static final String BOOTSTRAP_SERVERS = "kafka-bootstrap";
    private static final String SCHEMA_REG_URL = "schema-reg-url";
    private static final Class<? extends Serializer<Long>> KEY_SERIALIZER_TYPE =
            LongSerializer.class;
    private static final Class<? extends Serializer<String>> VAL_SERIALIZER_TYPE =
            StringSerializer.class;
    private static final Class<? extends Deserializer<Integer>> KEY_DESERIALIZER_TYPE =
            IntegerDeserializer.class;
    private static final Class<? extends Deserializer<Double>> VAL_DESERIALIZER_TYPE =
            DoubleDeserializer.class;
    private static final Class<? extends Serde<UUID>> KEY_SERDE_TYPE = Serdes.UUIDSerde.class;
    private static final Class<? extends Serde<Boolean>> VAL_SERDE_TYPE = Serdes.BooleanSerde.class;
    private Clients.ClientBuilder builder;

    @BeforeEach
    void setUp() {
        builder = Clients.builder(DOMAIN_ID, SERVICE_ID, BOOTSTRAP_SERVERS, SCHEMA_REG_URL);
    }

    @Test
    void shouldBuildProducerProps() {
        // When:
        final ProducerProperties<Long, String> props =
                builder.producer()
                        .withKeyType(Long.class)
                        .withValueSerializerType(StringSerializer.class)
                        .buildProperties();

        // Then:
        final Map<String, ?> expected =
                Map.of(
                        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                        BOOTSTRAP_SERVERS,
                        CommonClientConfigs.CLIENT_ID_CONFIG,
                        DOMAIN_ID + "." + SERVICE_ID + ".producer",
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                        KEY_SERIALIZER_TYPE,
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                        VAL_SERIALIZER_TYPE,
                        ProducerConfig.ACKS_CONFIG,
                        "all",
                        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        SCHEMA_REG_URL,
                        AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS,
                        false,
                        KafkaAvroSerializerConfig.USE_LATEST_VERSION,
                        true);
        assertThat(props.asMap(), is(expected));
    }

    @Test
    void shouldCustomizeProducerAcks() {
        // When:
        final ProducerProperties<Long, String> props =
                builder.producer()
                        .withKeyType(Long.class)
                        .withValueType(String.class)
                        .withAcks(false)
                        .buildProperties();

        // Then:
        assertThat(props.asMap(), hasEntry(ProducerConfig.ACKS_CONFIG, "1"));
    }

    @Test
    void shouldBuildLegacyProducerProps() {
        // When:
        final Map<String, Object> props =
                Clients.producerProperties(
                        DOMAIN_ID,
                        SERVICE_ID,
                        BOOTSTRAP_SERVERS,
                        SCHEMA_REG_URL,
                        KEY_SERIALIZER_TYPE,
                        VAL_SERIALIZER_TYPE,
                        true);

        // Then:
        final Map<String, ?> expected =
                Map.of(
                        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                        BOOTSTRAP_SERVERS,
                        CommonClientConfigs.CLIENT_ID_CONFIG,
                        DOMAIN_ID + "." + SERVICE_ID + ".producer",
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                        KEY_SERIALIZER_TYPE,
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                        VAL_SERIALIZER_TYPE,
                        ProducerConfig.ACKS_CONFIG,
                        "all",
                        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        SCHEMA_REG_URL,
                        AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS,
                        false,
                        KafkaAvroSerializerConfig.SCHEMA_REFLECTION_CONFIG,
                        true,
                        KafkaAvroSerializerConfig.USE_LATEST_VERSION,
                        true);
        assertThat(props, is(expected));
    }

    @Test
    void shouldNotAllowSerializerTypesToBeOverridden() {
        // Given:
        final Map<String, Object> overrides =
                Map.of(
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, VoidSerializer.class,
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VoidSerializer.class);

        // When:
        final ProducerProperties<Long, String> props =
                builder.withProps(overrides)
                        .producer()
                        .withKeyType(Long.class)
                        .withValueType(String.class)
                        .buildProperties();

        // Then:
        assertThat(
                props.asMap(),
                hasEntry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER_TYPE));
        assertThat(
                props.asMap(),
                hasEntry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VAL_SERIALIZER_TYPE));
    }

    @Test
    void shouldBuildConsumerProps() {
        // When:
        final ConsumerProperties<Integer, Double> props =
                builder.consumer()
                        .withKeyType(int.class)
                        .withValueType(Double.class)
                        .buildProperties();

        // Then:
        final Map<String, ?> expected =
                Map.of(
                        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                        BOOTSTRAP_SERVERS,
                        ConsumerConfig.CLIENT_ID_CONFIG,
                        DOMAIN_ID + "." + SERVICE_ID + ".consumer",
                        ConsumerConfig.GROUP_ID_CONFIG,
                        DOMAIN_ID + "." + SERVICE_ID + ".consumer-group",
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        "earliest",
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                        KEY_DESERIALIZER_TYPE,
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        VAL_DESERIALIZER_TYPE,
                        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        SCHEMA_REG_URL);
        assertThat(props.asMap(), is(expected));
    }

    @Test
    void shouldBuildLegacyConsumerProps() {
        // When:
        final Map<String, Object> props =
                Clients.consumerProperties(
                        DOMAIN_ID,
                        SERVICE_ID,
                        BOOTSTRAP_SERVERS,
                        SCHEMA_REG_URL,
                        KEY_DESERIALIZER_TYPE,
                        VAL_DESERIALIZER_TYPE,
                        true);

        // Then:
        final Map<String, ?> expected =
                Map.of(
                        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                        BOOTSTRAP_SERVERS,
                        ConsumerConfig.CLIENT_ID_CONFIG,
                        DOMAIN_ID + "." + SERVICE_ID + ".consumer",
                        ConsumerConfig.GROUP_ID_CONFIG,
                        DOMAIN_ID + "." + SERVICE_ID + ".consumer-group",
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        "earliest",
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                        KEY_DESERIALIZER_TYPE,
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        VAL_DESERIALIZER_TYPE,
                        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        SCHEMA_REG_URL,
                        AbstractKafkaSchemaSerDeConfig.SCHEMA_REFLECTION_CONFIG,
                        true);
        assertThat(props, is(expected));
    }

    @Test
    void shouldCustomizeAutoOffsetReset() {
        // When:
        final ConsumerProperties<Integer, Double> props =
                builder.consumer()
                        .withKeyType(int.class)
                        .withValueType(Double.class)
                        .withAutoOffsetReset(false)
                        .buildProperties();

        // Then:
        assertThat(props.asMap(), hasEntry(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"));
    }

    @Test
    void shouldNotAllowDeserializerTypesToBeOverridden() {
        // Given:
        final Map<String, Object> overrides =
                Map.of(
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class,
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class);

        // When:
        final ConsumerProperties<Integer, Double> props =
                builder.withProps(overrides)
                        .consumer()
                        .withKeyDeserializerType(KEY_DESERIALIZER_TYPE)
                        .withValueDeserializerType(VAL_DESERIALIZER_TYPE)
                        .buildProperties();

        // Then:
        assertThat(
                props.asMap(),
                hasEntry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KEY_DESERIALIZER_TYPE));
        assertThat(
                props.asMap(),
                hasEntry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VAL_DESERIALIZER_TYPE));
    }

    @Test
    void shouldBuildKStreamsProps() {
        // When:
        final KStreamsProperties<UUID, Boolean> props =
                builder.kstreams()
                        .withKeyType(UUID.class)
                        .withValueSerdeType(VAL_SERDE_TYPE)
                        .buildProperties();

        // Then:
        final Map<String, ?> expected =
                Map.of(
                        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                        BOOTSTRAP_SERVERS,
                        StreamsConfig.APPLICATION_ID_CONFIG,
                        DOMAIN_ID + "._private." + SERVICE_ID,
                        StreamsConfig.CLIENT_ID_CONFIG,
                        DOMAIN_ID + "." + SERVICE_ID + ".client",
                        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                        KEY_SERDE_TYPE,
                        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                        VAL_SERDE_TYPE,
                        StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,
                        10 * 1000L,
                        ProducerConfig.ACKS_CONFIG,
                        "all",
                        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        SCHEMA_REG_URL,
                        AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS,
                        false,
                        KafkaAvroSerializerConfig.USE_LATEST_VERSION,
                        true);
        assertThat(props.asMap(), is(expected));
    }

    @Test
    void shouldBuildLegacyKStreamsProps() {
        // When:
        final Map<String, Object> props =
                Clients.kstreamsProperties(
                        DOMAIN_ID,
                        SERVICE_ID,
                        BOOTSTRAP_SERVERS,
                        SCHEMA_REG_URL,
                        KEY_SERDE_TYPE,
                        VAL_SERDE_TYPE,
                        false);

        // Then:
        final Map<String, ?> expected =
                Map.ofEntries(
                        Map.entry(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS),
                        Map.entry(
                                StreamsConfig.APPLICATION_ID_CONFIG,
                                DOMAIN_ID + "._private." + SERVICE_ID),
                        Map.entry(
                                StreamsConfig.CLIENT_ID_CONFIG,
                                DOMAIN_ID + "." + SERVICE_ID + ".client"),
                        Map.entry(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, KEY_SERDE_TYPE),
                        Map.entry(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, VAL_SERDE_TYPE),
                        Map.entry(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000L),
                        Map.entry(ProducerConfig.ACKS_CONFIG, "1"),
                        Map.entry(
                                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                                SCHEMA_REG_URL),
                        Map.entry(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false),
                        Map.entry(KafkaAvroSerializerConfig.SCHEMA_REFLECTION_CONFIG, true),
                        Map.entry(KafkaAvroSerializerConfig.USE_LATEST_VERSION, true));
        assertThat(props, is(expected));
    }

    @Test
    void shouldCustomizeKStreamAcks() {
        // When:
        final KStreamsProperties<UUID, Boolean> props =
                builder.kstreams()
                        .withKeyType(UUID.class)
                        .withValueSerdeType(VAL_SERDE_TYPE)
                        .withAcks(false)
                        .buildProperties();

        // Then:
        assertThat(props.asMap(), hasEntry(ProducerConfig.ACKS_CONFIG, "1"));
    }

    @Test
    void shouldNotAllowSerdeTypesToBeOverridden() {
        // Given:
        final Map<String, Object> overrides =
                Map.of(
                        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, VoidDeserializer.class,
                        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, VoidDeserializer.class);

        // When:
        final KStreamsProperties<UUID, Boolean> props =
                builder.withProps(overrides)
                        .kstreams()
                        .withKeyType(UUID.class)
                        .withValueSerdeType(VAL_SERDE_TYPE)
                        .withAcks(false)
                        .buildProperties();

        // Then:
        assertThat(
                props.asMap(),
                hasEntry(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, KEY_SERDE_TYPE));
        assertThat(
                props.asMap(),
                hasEntry(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, VAL_SERDE_TYPE));
    }

    @Test
    void shouldApplyOverridesInOrder() {
        // Given:
        final Map<String, Object> override1 = Map.of("custom", 120);
        final Map<String, Object> override2 = Map.of("custom", 129);

        // When:
        final ProducerProperties<Long, String> props =
                builder.withProps(override1)
                        .withProps(override2)
                        .producer()
                        .withKeyType(Long.class)
                        .withValueType(String.class)
                        .buildProperties();

        // Then:
        assertThat(props.asMap(), hasEntry("custom", 129));
    }
}
