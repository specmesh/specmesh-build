package io.specmesh.kafka;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public final class Clients {
    private Clients(){}

    public static <K, V> KafkaProducer<K, V> producer(final Class<K> keyClass,
                                                      final Class<V> valueClass,
                                                      final Map<String, Object> producerProperties
                                                      ) {
        return new KafkaProducer<>(producerProperties);
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    @NotNull
    public static Map<String, Object> producerProperties(final String domainId,
                                                 final String serviceId,
                                               final String bootstrapServers,
                                               final String schemaRegistryUrl,
                                               final Class<?> keySerializerClass,
                                               final Class<?> valueSerializerClass,
                                               final boolean acksAll,
                                               final Map<String, Object> providedProperties) {
        return mergeMaps(getClientProperties(domainId, bootstrapServers),
                Map.of(
                        AdminClientConfig.CLIENT_ID_CONFIG, domainId + "." + serviceId + ".producer",
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass.getCanonicalName(),
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass.getCanonicalName(),
                        ProducerConfig.ACKS_CONFIG, acksAll? "all" : "1",
                        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        schemaRegistryUrl,
                        // AUTO-REG should be false to allow schemas to be published by controlled processes
                        AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "false",
                        // schema-reflect MUST be true when writing Java objects (otherwise you send a datum-container instead of a Pojo)
                        KafkaAvroSerializerConfig.SCHEMA_REFLECTION_CONFIG, "true",
                        KafkaAvroSerializerConfig.USE_LATEST_VERSION, "true"
                ),
                providedProperties);
    }


    public static  <K, V> KafkaConsumer<K, V> consumer(final Class<K> keyClass,
                                                       final Class<V> valueClass,
                                                       final Map<String, Object> consumerProperties) {
        return new KafkaConsumer<>(consumerProperties);
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    @NotNull
    public static Map<String, Object> consumerProperties(final String domainId,
                                               final String serviceId,
                                               final String bootstrapServers,
                                               final String schemaRegistryUrl,
                                               final Class<?> keyDeserializerClass,
                                               final Class<?> valueDeserializerClass,
                                               final boolean autoOffsetResetEarliest,
                                               final Map<String, Object> providedProperties) {
        return mergeMaps(getClientProperties(domainId, bootstrapServers),
                Map.of(
                        ConsumerConfig.CLIENT_ID_CONFIG, domainId + "." + serviceId + ".consumer",
                        ConsumerConfig.GROUP_ID_CONFIG, domainId + "." + serviceId + ".consumer-group",
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetResetEarliest ? "earliest" : "latest",
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass.getCanonicalName(),
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass.getCanonicalName(),
                        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl,
                        AbstractKafkaSchemaSerDeConfig.SCHEMA_REFLECTION_CONFIG, "true"
                ),
                providedProperties);
    }

    private static Properties getClientProperties(final String domainId, final String bootstrapServers) {
        final Properties adminClientProperties = new Properties();
        adminClientProperties.put(AdminClientConfig.CLIENT_ID_CONFIG, domainId);
        adminClientProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return adminClientProperties;
    }

    @SuppressWarnings("rawtypes")
    private static Map<String, Object> mergeMaps(final Map... manyMaps) {
        final HashMap<String, Object> mutableMap = new HashMap<>();
        Arrays.stream(manyMaps).forEach(mutableMap::putAll);
        return  mutableMap;
    }
}
