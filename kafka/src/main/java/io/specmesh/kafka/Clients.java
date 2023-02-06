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

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;

/** Factory for Kafka clients */
public final class Clients {
    private Clients() {}

    /**
     * Create a Kafka producer
     *
     * @param keyClass the type of the key
     * @param valueClass the type of the value
     * @param producerProperties the properties
     * @param <K> the type of the key
     * @param <V> the type of the value
     * @return the producer
     */
    public static <K, V> KafkaProducer<K, V> producer(
            final Class<K> keyClass,
            final Class<V> valueClass,
            final Map<String, Object> producerProperties) {
        return new KafkaProducer<>(producerProperties);
    }

    /**
     * Create a map of producer properties with sensible defaults.
     *
     * @param domainId the domain id, used to scope resource names.
     * @param serviceId the name of the service
     * @param bootstrapServers bootstrap servers config
     * @param schemaRegistryUrl url of schema registry
     * @param keySerializerClass type of key serializer
     * @param valueSerializerClass type of value serializer
     * @param acksAll require acks from all replicas?
     * @param additionalProperties additional props
     * @return props
     */
    @SuppressWarnings("checkstyle:ParameterNumber")
    @SafeVarargs
    public static Map<String, Object> producerProperties(
            final String domainId,
            final String serviceId,
            final String bootstrapServers,
            final String schemaRegistryUrl,
            final Class<?> keySerializerClass,
            final Class<?> valueSerializerClass,
            final boolean acksAll,
            final Map<String, Object>... additionalProperties) {
        final Map<String, Object> props = clientProperties(domainId, bootstrapServers);
        props.putAll(
                Map.of(
                        AdminClientConfig.CLIENT_ID_CONFIG,
                        domainId + "." + serviceId + ".producer",
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                        keySerializerClass.getCanonicalName(),
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                        valueSerializerClass.getCanonicalName(),
                        ProducerConfig.ACKS_CONFIG,
                        acksAll ? "all" : "1",
                        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        schemaRegistryUrl,
                        // AUTO-REG should be false to allow schemas to be published by controlled
                        // processes
                        AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS,
                        "false",
                        // schema-reflect MUST be true when writing Java objects (otherwise you send
                        // a
                        // datum-container instead of a Pojo)
                        KafkaAvroSerializerConfig.SCHEMA_REFLECTION_CONFIG,
                        "true",
                        KafkaAvroSerializerConfig.USE_LATEST_VERSION,
                        "true"));
        addAdditional(props, additionalProperties);
        return props;
    }

    /**
     * Create props for KStream app with sensible defaults.
     *
     * @param domainId the domain id, used to scope resource names.
     * @param serviceId the name of the service
     * @param bootstrapServers bootstrap servers config
     * @param schemaRegistryUrl url of schema registry
     * @param keySerdeClass type of key serde
     * @param valueSerdeClass type of value serde
     * @param acksAll require acks from all replicas?
     * @param additionalProperties additional properties
     * @return the streams properties.
     */
    @SuppressWarnings("checkstyle:ParameterNumber")
    @SafeVarargs
    public static Map<String, Object> kstreamsProperties(
            final String domainId,
            final String serviceId,
            final String bootstrapServers,
            final String schemaRegistryUrl,
            final Class<?> keySerdeClass,
            final Class<?> valueSerdeClass,
            final boolean acksAll,
            final Map<String, Object>... additionalProperties) {

        final Map<String, Object> props = clientProperties(domainId, bootstrapServers);
        props.putAll(
                Map.of(
                        StreamsConfig.APPLICATION_ID_CONFIG,
                        domainId + "." + serviceId,
                        StreamsConfig.CLIENT_ID_CONFIG,
                        domainId + "." + serviceId + ".client",
                        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                        keySerdeClass.getName(),
                        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                        valueSerdeClass.getName(),
                        // Records should be flushed every 10 seconds. This is less than the default
                        // in order to keep this example interactive.
                        StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,
                        10 * 1000,
                        ProducerConfig.ACKS_CONFIG,
                        acksAll ? "all" : "1",
                        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        schemaRegistryUrl,
                        // AUTO-REG should be false to allow schemas to be published by controlled
                        // processes
                        AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS,
                        "false",
                        // schema-reflect MUST be true when writing Java objects
                        // (otherwise you send a datum-container (avro) or dynamic record (proto)
                        // instead of a Pojo)
                        KafkaAvroSerializerConfig.SCHEMA_REFLECTION_CONFIG,
                        "true",
                        KafkaAvroSerializerConfig.USE_LATEST_VERSION,
                        "true"));
        addAdditional(props, additionalProperties);
        return props;
    }

    /**
     * Create a Kafka consumer
     *
     * @param keyClass the type of the key
     * @param valueClass the type of the value
     * @param consumerProperties the properties
     * @param <K> the type of the key
     * @param <V> the type of the value
     * @return the producer
     */
    public static <K, V> KafkaConsumer<K, V> consumer(
            final Class<K> keyClass,
            final Class<V> valueClass,
            final Map<String, Object> consumerProperties) {
        return new KafkaConsumer<>(consumerProperties);
    }

    /**
     * Create a map of consumer properties with sensible defaults.
     *
     * @param domainId the domain id, used to scope resource names.
     * @param serviceId the name of the service
     * @param bootstrapServers bootstrap servers config
     * @param schemaRegistryUrl url of schema registry
     * @param keyDeserializerClass type of key deserializer
     * @param valueDeserializerClass type of value deserializer
     * @param autoOffsetResetEarliest reset to earliest offset if no stored offsets?
     * @param additionalProperties additional properties
     * @return props
     */
    @SuppressWarnings("checkstyle:ParameterNumber")
    @SafeVarargs
    public static Map<String, Object> consumerProperties(
            final String domainId,
            final String serviceId,
            final String bootstrapServers,
            final String schemaRegistryUrl,
            final Class<?> keyDeserializerClass,
            final Class<?> valueDeserializerClass,
            final boolean autoOffsetResetEarliest,
            final Map<String, Object>... additionalProperties) {
        final Map<String, Object> props = clientProperties(domainId, bootstrapServers);
        props.putAll(
                Map.of(
                        ConsumerConfig.CLIENT_ID_CONFIG,
                        domainId + "." + serviceId + ".consumer",
                        ConsumerConfig.GROUP_ID_CONFIG,
                        domainId + "." + serviceId + ".consumer-group",
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        autoOffsetResetEarliest ? "earliest" : "latest",
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                        keyDeserializerClass.getCanonicalName(),
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        valueDeserializerClass.getCanonicalName(),
                        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        schemaRegistryUrl,
                        AbstractKafkaSchemaSerDeConfig.SCHEMA_REFLECTION_CONFIG,
                        "true"));
        addAdditional(props, additionalProperties);
        return props;
    }

    private static Map<String, Object> clientProperties(
            final String domainId, final String bootstrapServers) {
        final Map<String, Object> basicProps = new HashMap<>();
        basicProps.put(AdminClientConfig.CLIENT_ID_CONFIG, domainId);
        basicProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return basicProps;
    }

    @SafeVarargs
    private static void addAdditional(
            final Map<String, Object> props, final Map<String, Object>... additionalProperties) {
        for (final Map<String, Object> additional : additionalProperties) {
            props.putAll(additional);
        }
    }
}
