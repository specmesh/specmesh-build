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

import static java.util.Objects.requireNonNull;

import com.google.protobuf.MessageLiteOrBuilder;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.serialization.BooleanDeserializer;
import org.apache.kafka.common.serialization.BooleanSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.FloatDeserializer;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.ShortDeserializer;
import org.apache.kafka.common.serialization.ShortSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.UUIDDeserializer;
import org.apache.kafka.common.serialization.UUIDSerializer;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.apache.kafka.common.serialization.VoidSerializer;
import org.apache.kafka.streams.StreamsConfig;

/** Factory for Kafka clients */
public final class Clients {

    public static final String SASL_MECHANISM = "sasl.mechanism";
    public static final String SASL_JAAS_CONFIG = "sasl.jaas.config";
    public static final String CONFIG_PROPERTIES = "config.properties";
    public static final String NONE = "none";
    public static final String PLAIN = "PLAIN";
    public static final String SASL_PLAINTEXT = "SASL_PLAINTEXT";
    public static final int TIMEOUT = 30;

    private static final Map<Class<?>, SerdeTypes<?>> STD_SERDE =
            Map.ofEntries(
                    Map.entry(
                            Long.class,
                            typeMetaData(
                                    LongSerializer.class,
                                    LongDeserializer.class,
                                    Serdes.LongSerde.class)),
                    Map.entry(
                            long.class,
                            typeMetaData(
                                    LongSerializer.class,
                                    LongDeserializer.class,
                                    Serdes.LongSerde.class)),
                    Map.entry(
                            Integer.class,
                            typeMetaData(
                                    IntegerSerializer.class,
                                    IntegerDeserializer.class,
                                    Serdes.IntegerSerde.class)),
                    Map.entry(
                            int.class,
                            typeMetaData(
                                    IntegerSerializer.class,
                                    IntegerDeserializer.class,
                                    Serdes.IntegerSerde.class)),
                    Map.entry(
                            Short.class,
                            typeMetaData(
                                    ShortSerializer.class,
                                    ShortDeserializer.class,
                                    Serdes.ShortSerde.class)),
                    Map.entry(
                            short.class,
                            typeMetaData(
                                    ShortSerializer.class,
                                    ShortDeserializer.class,
                                    Serdes.ShortSerde.class)),
                    Map.entry(
                            Float.class,
                            typeMetaData(
                                    FloatSerializer.class,
                                    FloatDeserializer.class,
                                    Serdes.FloatSerde.class)),
                    Map.entry(
                            float.class,
                            typeMetaData(
                                    FloatSerializer.class,
                                    FloatDeserializer.class,
                                    Serdes.FloatSerde.class)),
                    Map.entry(
                            Double.class,
                            typeMetaData(
                                    DoubleSerializer.class,
                                    DoubleDeserializer.class,
                                    Serdes.DoubleSerde.class)),
                    Map.entry(
                            double.class,
                            typeMetaData(
                                    DoubleSerializer.class,
                                    DoubleDeserializer.class,
                                    Serdes.DoubleSerde.class)),
                    Map.entry(
                            Boolean.class,
                            typeMetaData(
                                    BooleanSerializer.class,
                                    BooleanDeserializer.class,
                                    Serdes.BooleanSerde.class)),
                    Map.entry(
                            boolean.class,
                            typeMetaData(
                                    BooleanSerializer.class,
                                    BooleanDeserializer.class,
                                    Serdes.BooleanSerde.class)),
                    Map.entry(
                            String.class,
                            typeMetaData(
                                    StringSerializer.class,
                                    StringDeserializer.class,
                                    Serdes.StringSerde.class)),
                    Map.entry(
                            UUID.class,
                            typeMetaData(
                                    UUIDSerializer.class,
                                    UUIDDeserializer.class,
                                    Serdes.UUIDSerde.class)),
                    Map.entry(
                            void.class,
                            typeMetaData(
                                    VoidSerializer.class,
                                    VoidDeserializer.class,
                                    Serdes.VoidSerde.class)),
                    Map.entry(
                            Void.class,
                            typeMetaData(
                                    VoidSerializer.class,
                                    VoidDeserializer.class,
                                    Serdes.VoidSerde.class)));

    private Clients() {}

    /**
     * AdminClient access
     *
     * @param brokerUrl broker address
     * @param username - user
     * @param secret - secrets
     * @return = adminClient
     */
    public static Admin adminClient(
            final String brokerUrl, final String username, final String secret) {

        try {
            final Map<String, Object> properties = new HashMap<>();
            properties.put(AdminClientConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
            properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);

            if (username != null) {
                properties.putAll(clientSaslAuthProperties(username, secret));
            }

            if (!System.getProperty(CONFIG_PROPERTIES, NONE).equals(NONE)) {
                return validate(
                        AdminClient.create(
                                loadPropertiesFile(
                                        properties, System.getProperty(CONFIG_PROPERTIES))));
            } else {
                return validate(AdminClient.create(properties));
            }
        } catch (Exception ex) {
            throw new ClientsException(
                    "cannot load:" + brokerUrl + " with username:" + username, ex);
        }
    }

    private static Admin validate(final AdminClient adminClient) {
        try {
            adminClient.describeCluster().clusterId().get(TIMEOUT, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(
                    "AdminClient cannot access the cluster (client.describeCluster), "
                            + "check connection-url/credentials/broker logs",
                    e);
        }
        return adminClient;
    }

    private static Map<String, Object> loadPropertiesFile(
            final Map<String, Object> defaultProperties, final String propertyFilename) {
        try (InputStream input = new FileInputStream(propertyFilename)) {

            final var prop = new Properties();
            prop.putAll(defaultProperties);
            prop.load(input);
            return prop.entrySet().stream()
                    .collect(
                            Collectors.toMap(
                                    entry -> entry.getKey().toString(), Map.Entry::getValue));
        } catch (IOException e) {
            throw new RuntimeException("Failed to load properties file:" + propertyFilename, e);
        }
    }

    /**
     * setup sasl_plain auth creds
     *
     * @param principal user name
     * @param secret secret
     * @return client creds map
     */
    public static Map<String, Object> clientSaslAuthProperties(
            final String principal, final String secret) {
        if (isPrincipalSpecified(principal)) {
            return Map.of(
                    SASL_MECHANISM,
                    System.getProperty(SASL_MECHANISM, PLAIN),
                    AdminClientConfig.SECURITY_PROTOCOL_CONFIG,
                    System.getProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, SASL_PLAINTEXT),
                    SASL_JAAS_CONFIG,
                    System.getProperty(SASL_JAAS_CONFIG, buildJaasConfig(principal, secret)));
        } else {
            return Map.of();
        }
    }

    private static boolean isPrincipalSpecified(final String principal) {
        return principal != null && !principal.isBlank();
    }

    private static String buildJaasConfig(final String userName, final String password) {
        return PlainLoginModule.class.getCanonicalName()
                + " required "
                + "username=\""
                + userName
                + "\" password=\""
                + password
                + "\";";
    }

    /**
     * @deprecated use {@link #schemaRegistryClient(String, String, String)}.
     */
    @Deprecated(forRemoval = true, since = "0.10.1")
    public static Optional<SchemaRegistryClient> schemaRegistryClient(
            final boolean srEnabled,
            final String schemaRegistryUrl,
            final String srApiKey,
            final String srApiSecret) {
        if (srEnabled && schemaRegistryUrl != null) {
            return Optional.of(schemaRegistryClient(schemaRegistryUrl, srApiKey, srApiSecret));
        } else {
            return Optional.empty();
        }
    }

    public static SchemaRegistryClient schemaRegistryClient(
            final String schemaRegistryUrl, final String srApiKey, final String srApiSecret) {
        final Map<String, Object> properties = new HashMap<>();
        if (srApiKey != null && !srApiKey.isBlank()) {
            properties.put(SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
            properties.put(
                    SchemaRegistryClientConfig.USER_INFO_CONFIG, srApiKey + ":" + srApiSecret);
        }
        return new CachedSchemaRegistryClient(
                requireNonNull(schemaRegistryUrl, "schemaRegistryUrl"), 5, properties);
    }

    /**
     * Obtain a new client builder, which can be used to build multiple clients.
     *
     * @param domainId the domain id, used to scope resource names and ids.
     * @param serviceId the name of the service
     * @param bootstrapServers the kafka bootstrap servers
     * @param schemaRegistryUrl the url of schema registry
     * @return new client builder instance
     */
    public static ClientBuilder builder(
            final String domainId,
            final String serviceId,
            final String bootstrapServers,
            final String schemaRegistryUrl) {
        return new ClientBuilder(domainId, serviceId, bootstrapServers, schemaRegistryUrl);
    }

    /**
     * Type safe container of client properties.
     *
     * <p>The type parameters match the type of the key and value serde types used when constructing
     * the properties.
     *
     * @param <K> the type of the key
     * @param <V> the type of the value
     */
    @SuppressWarnings("unused")
    public abstract static class ClientProperties<K, V> {

        private final Map<String, ?> properties;

        private ClientProperties(final Map<String, ?> properties) {
            this.properties = Map.copyOf(requireNonNull(properties, "properties"));
        }

        public Map<String, Object> asMap() {
            return Map.copyOf(properties);
        }

        public Properties asProperties() {
            final Properties props = new Properties();
            props.putAll(properties);
            return props;
        }
    }

    /**
     * Type-safe producer config
     *
     * <p>The type parameters match the type of the key and value serializer types used when
     * constructing the properties.
     *
     * @param <K> the type of the key
     * @param <V> the type of the value
     */
    public static final class ProducerProperties<K, V> extends ClientProperties<K, V> {

        private ProducerProperties(final Map<String, ?> properties) {
            super(properties);
        }
    }

    /**
     * Type-safe consumer config
     *
     * <p>The type parameters match the type of the key and value deserializer types used when
     * constructing the properties.
     *
     * @param <K> the type of the key
     * @param <V> the type of the value
     */
    public static final class ConsumerProperties<K, V> extends ClientProperties<K, V> {

        private ConsumerProperties(final Map<String, ?> properties) {
            super(properties);
        }
    }

    /**
     * Type-safe kafka streams config
     *
     * <p>The type parameters match the type of the key and value serde types used when constructing
     * the properties.
     *
     * @param <K> the type of the key
     * @param <V> the type of the value
     */
    public static final class KStreamsProperties<K, V> extends ClientProperties<K, V> {

        private KStreamsProperties(final Map<String, ?> properties) {
            super(properties);
        }
    }

    /** Type used to, erm, build clients. */
    public static final class ClientBuilder {

        private static final Map<String, ?> BASE_PROPS =
                Map.of(
                        // schema-reflect MUST be true when writing Java objects (otherwise you send
                        // a datum-container instead of a Pojo)
                        AbstractKafkaSchemaSerDeConfig.SCHEMA_REFLECTION_CONFIG, true);

        private final String domainId;
        private final String serviceId;
        private final Map<String, ?> commonProps;
        private final Map<String, Object> additional;

        private ClientBuilder(
                final String domainId,
                final String serviceId,
                final String bootstrapServers,
                final String schemaRegistryUrl) {
            this.domainId = requireNonNull(domainId, "domainId");
            this.serviceId = requireNonNull(serviceId, "serviceId");
            this.commonProps =
                    Map.of(
                            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                            bootstrapServers,
                            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                            schemaRegistryUrl);
            this.additional = new HashMap<>();
        }

        private ClientBuilder(
                final String domainId,
                final String serviceId,
                final Map<String, ?> commonProps,
                final Map<String, Object> additional) {
            this.domainId = requireNonNull(domainId, "domainId");
            this.serviceId = requireNonNull(serviceId, "serviceId");
            this.commonProps = Map.copyOf(requireNonNull(commonProps, "commonProps"));
            this.additional = new HashMap<>(additional);
        }

        /**
         * Add additional client properties.
         *
         * <p>Properties that change teh serde type of the clients will be ignored.
         *
         * @param additional additional client properties.
         * @return a new builder instance with the additional properties set.
         */
        public ClientBuilder withProps(final Map<String, ?> additional) {
            final Map<String, Object> newAdditional = new HashMap<>(this.additional);
            newAdditional.putAll(additional);
            return new ClientBuilder(domainId, serviceId, commonProps, newAdditional);
        }

        /**
         * Add additional client property.
         *
         * <p>Properties that change teh serde type of the clients will be ignored.
         *
         * @param key key of the additional client property.
         * @param value value of the additional client property.
         * @return a new builder instance with the additional properties set.
         */
        public ClientBuilder withProp(final String key, final Object value) {
            return withProps(Map.of(key, value));
        }

        /**
         * @return a new producer builder, which can be used to build a producer or producer config
         */
        public ProducerBuilder<Void, Void> producer() {
            return new ProducerBuilder<>(this);
        }

        /**
         * @return a new consumer builder, which can be used to build a consumer or consumer config
         */
        public ConsumerBuilder<Void, Void> consumer() {
            return new ConsumerBuilder<>(this);
        }

        /**
         * @return a new kstreams builder, which can be used to build kstreams config
         */
        public <V, K> KStreamsBuilder<K, V> kstreams() {
            return new KStreamsBuilder<>(this);
        }

        private String clientIdentifier(final String lastPart) {
            return domainId + "." + serviceId + "." + lastPart;
        }

        private Map<String, ?> overrides(final String... nonOverridableKeys) {
            final HashMap<String, Object> allowed = new HashMap<>(additional);
            for (final String nonOverridableKey : nonOverridableKeys) {
                allowed.keySet().remove(nonOverridableKey);
            }
            return allowed;
        }

        private Map<String, Object> baseProps() {
            final Map<String, Object> props = new HashMap<>(BASE_PROPS);
            props.putAll(commonProps);
            return props;
        }
    }

    /**
     * Type-safe builder of producers and producer config
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    @SuppressWarnings({"unchecked", "OptionalUsedAsFieldOrParameterType"})
    public static final class ProducerBuilder<K, V> {

        private static final Map<String, ?> DEFAULT_PRODUCER_PROPS =
                Map.of(
                        // Disable auto-reg to allow schemas to be published by controlled processes
                        AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS,
                        false,
                        AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION,
                        true);

        private final ClientBuilder clientBuilder;
        private Optional<Class<? extends Serializer<K>>> keySerializer = Optional.empty();
        private Optional<Class<? extends Serializer<V>>> valSerializer = Optional.empty();
        private boolean acksAll = true;

        private ProducerBuilder(final ClientBuilder clientBuilder) {
            this.clientBuilder = requireNonNull(clientBuilder, "clientBuilder");
        }

        /**
         * Set the key serializer from the supplied {@code type}.
         *
         * <p>Method will attempt to determine the correct serializer type from the supplied {@code
         * type}. throwing an exception if there is insufficient type information to this to be
         * determined.
         *
         * <p>The supplied {@code type} can be on of the types supported by the standard Kafka
         * serializers, (see {@link #STD_SERDE}), or a generated Protobuf or Avro type.
         *
         * <p>If this method throws due to insufficient type information, use on of the {@link
         * #withKeySerializerType} methods to supply more information.
         *
         * @param type the new key type
         * @return self.
         * @param <T> the new key type
         */
        public <T> ProducerBuilder<T, V> withKeyType(final Class<T> type) {
            return withKeySerializerType(serializerFor(type));
        }

        /**
         * Set the key serializer to the supplied {@code type}.
         *
         * @param type the key serializer type
         * @return self.
         * @param <T> the new key type
         */
        public <T> ProducerBuilder<T, V> withKeySerializerType(
                final Class<? extends Serializer<T>> type) {
            final ProducerBuilder<T, V> adjusted = (ProducerBuilder<T, V>) this;
            adjusted.keySerializer = Optional.of(type);
            return (ProducerBuilder<T, V>) this;
        }

        /**
         * Set the key serializer to a non-generic type.
         *
         * <p>Some serializers, e.g. {@link KafkaAvroSerializer}, do not accept template parameters
         * that represent the type they serialize. This method variant avoids the need for casts in
         * code by accepting the target type and performing the cast.
         *
         * @param serializerType the serializer type
         * @param type the key type the serializer handles
         * @return self
         * @param <T> the new key type
         */
        @SuppressWarnings("rawtypes")
        public <T> ProducerBuilder<T, V> withKeySerializerType(
                final Class<? extends Serializer> serializerType, final Class<T> type) {
            return withKeySerializerType(castNonGeneric(serializerType, type));
        }

        /**
         * Set the value serializer from the supplied {@code type}.
         *
         * <p>Method will attempt to determine the correct serializer type from the supplied {@code
         * type}. throwing an exception if there is insufficient type information to this to be
         * determined.
         *
         * <p>The supplied {@code type} can be on of the types supported by the standard Kafka
         * serializers, (see {@link #STD_SERDE}), or a generated Protobuf or Avro type.
         *
         * <p>If this method throws due to insufficient type information, use on of the {@link
         * #withValueSerializerType} methods to supply more information.
         *
         * @param type the new value type
         * @return self.
         * @param <T> the new value type
         */
        public <T> ProducerBuilder<K, T> withValueType(final Class<T> type) {
            return withValueSerializerType(serializerFor(type));
        }

        /**
         * Set the value serializer to the supplied {@code type}.
         *
         * @param type the value serializer type
         * @return self.
         * @param <T> the value key type
         */
        public <T> ProducerBuilder<K, T> withValueSerializerType(
                final Class<? extends Serializer<T>> type) {
            final ProducerBuilder<K, T> adjusted = (ProducerBuilder<K, T>) this;
            adjusted.valSerializer = Optional.of(type);
            return (ProducerBuilder<K, T>) this;
        }

        /**
         * Set the value serializer to a non-generic type.
         *
         * <p>Some serializers, e.g. {@link KafkaAvroSerializer}, do not accept template parameters
         * that represent the type they serialize. This method variant avoids the need for casts in
         * code by accepting the target type and performing the cast.
         *
         * @param serializerType the serializer type
         * @param type the value type the serializer handles
         * @return self
         * @param <T> the new value type
         */
        @SuppressWarnings("rawtypes")
        public <T> ProducerBuilder<K, T> withValueSerializerType(
                final Class<? extends Serializer> serializerType, final Class<T> type) {
            return withValueSerializerType(castNonGeneric(serializerType, type));
        }

        /**
         * Override the default acks required of {@code all}.
         *
         * @param acksAll if true, {@code all} acks are required, otherwise {@code 1}.
         * @return self
         */
        public ProducerBuilder<K, V> withAcks(final boolean acksAll) {
            this.acksAll = acksAll;
            return this;
        }

        /**
         * @return type-safe producer properties.
         */
        public ProducerProperties<K, V> buildProperties() {
            final Class<? extends Serializer<K>> keySer =
                    keySerializer.orElseThrow(
                            () ->
                                    new ClientsException(
                                            "key serializer not set. Call either withKeyType or"
                                                    + " withKeySerializerType."));
            final Class<? extends Serializer<V>> valSer =
                    valSerializer.orElseThrow(
                            () ->
                                    new ClientsException(
                                            "value serializer not set. Call either withValueType or"
                                                    + " withValueSerializerType."));

            final Map<String, Object> props = clientBuilder.baseProps();
            props.putAll(DEFAULT_PRODUCER_PROPS);
            props.putAll(
                    Map.of(
                            CommonClientConfigs.CLIENT_ID_CONFIG,
                            clientBuilder.clientIdentifier("producer"),
                            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                            keySer,
                            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                            valSer,
                            ProducerConfig.ACKS_CONFIG,
                            acksAll ? "all" : "1"));
            props.putAll(
                    clientBuilder.overrides(
                            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
            return new ProducerProperties<>(props);
        }

        /**
         * @return a new Kafka producer
         */
        public Producer<K, V> build() {
            return producer(buildProperties());
        }

        private static <T> Class<? extends Serializer<T>> serializerFor(final Class<T> type) {
            if (MessageLiteOrBuilder.class.isAssignableFrom(type)) {
                return castNonGeneric(KafkaProtobufSerializer.class, type);
            }

            if (GenericRecord.class.isAssignableFrom(type)) {
                return castNonGeneric(KafkaAvroSerializer.class, type);
            }

            return stdSerdeType(type).serializer;
        }

        @SuppressWarnings({"unused", "rawtypes"})
        private static <T> Class<? extends Serializer<T>> castNonGeneric(
                final Class<? extends Serializer> serializerType, final Class<T> type) {
            return (Class<? extends Serializer<T>>) serializerType;
        }
    }

    /**
     * Type-safe builder of consumer and consumer config
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    @SuppressWarnings({"unchecked", "OptionalUsedAsFieldOrParameterType"})
    public static final class ConsumerBuilder<K, V> {

        private final ClientBuilder clientBuilder;
        private Optional<Class<? extends Deserializer<K>>> keyDeserializer = Optional.empty();
        private Optional<Class<? extends Deserializer<V>>> valDeserializer = Optional.empty();
        private boolean autoOffsetResetEarliest = true;

        private ConsumerBuilder(final ClientBuilder clientBuilder) {
            this.clientBuilder = requireNonNull(clientBuilder, "clientBuilder");
        }

        /**
         * Set the key deserializer from the supplied {@code type}.
         *
         * <p>Method will attempt to determine the correct deserializer type from the supplied
         * {@code type}. throwing an exception if there is insufficient type information to this to
         * be determined.
         *
         * <p>The supplied {@code type} can be on of the types supported by the standard Kafka
         * deserializers, (see {@link #STD_SERDE}), or a generated Protobuf or Avro type.
         *
         * <p>If this method throws due to insufficient type information, use on of the {@link
         * #withKeyDeserializerType} methods to supply more information.
         *
         * @param type the new key type
         * @return self.
         * @param <T> the new key type
         */
        public <T> ConsumerBuilder<T, V> withKeyType(final Class<T> type) {
            return withKeyDeserializerType(deserializerFor(type));
        }

        /**
         * Set the key deserializer to the supplied {@code type}.
         *
         * @param type the key deserializer type
         * @return self.
         * @param <T> the new key type
         */
        public <T> ConsumerBuilder<T, V> withKeyDeserializerType(
                final Class<? extends Deserializer<T>> type) {
            final ConsumerBuilder<T, V> adjusted = (ConsumerBuilder<T, V>) this;
            adjusted.keyDeserializer = Optional.of(type);
            return (ConsumerBuilder<T, V>) this;
        }

        /**
         * Set the key deserializer to a non-generic type.
         *
         * <p>Some deserializers, e.g. {@link KafkaAvroDeserializer}, do not accept template
         * parameters that represent the type they deserialize. This method variant avoids the need
         * for casts in code by accepting the target type and performing the cast.
         *
         * @param deserializerType the deserializer type
         * @param type the key type the deserializer handles
         * @return self
         * @param <T> the new key type
         */
        @SuppressWarnings("rawtypes")
        public <T> ConsumerBuilder<T, V> withKeyDeserializerType(
                final Class<? extends Deserializer> deserializerType, final Class<T> type) {
            return withKeyDeserializerType(castNonGeneric(deserializerType, type));
        }

        /**
         * Set the value deserializer from the supplied {@code type}.
         *
         * <p>Method will attempt to determine the correct deserializer type from the supplied
         * {@code type}. throwing an exception if there is insufficient type information to this to
         * be determined.
         *
         * <p>The supplied {@code type} can be on of the types supported by the standard Kafka
         * deserializers, (see {@link #STD_SERDE}), or a generated Protobuf or Avro type.
         *
         * <p>If this method throws due to insufficient type information, use on of the {@link
         * #withValueDeserializerType} methods to supply more information.
         *
         * @param type the new value type
         * @return self.
         * @param <T> the new value type
         */
        public <T> ConsumerBuilder<K, T> withValueType(final Class<T> type) {
            return withValueDeserializerType(deserializerFor(type));
        }

        /**
         * Set the value deserializer to the supplied {@code type}.
         *
         * @param type the value deserializer type
         * @return self.
         * @param <T> the new value type
         */
        public <T> ConsumerBuilder<K, T> withValueDeserializerType(
                final Class<? extends Deserializer<T>> type) {
            final ConsumerBuilder<K, T> adjusted = (ConsumerBuilder<K, T>) this;
            adjusted.valDeserializer = Optional.of(type);
            return (ConsumerBuilder<K, T>) this;
        }

        /**
         * Set the value deserializer to a non-generic type.
         *
         * <p>Some deserializers, e.g. {@link KafkaAvroDeserializer}, do not accept template
         * parameters that represent the type they deserialize. This method variant avoids the need
         * for casts in code by accepting the target type and performing the cast.
         *
         * @param deserializerType the deserializer type
         * @param type the value type the deserializer handles
         * @return self
         * @param <T> the new value type
         */
        @SuppressWarnings("rawtypes")
        public <T> ConsumerBuilder<K, T> withValueDeserializerType(
                final Class<? extends Deserializer> deserializerType, final Class<T> type) {
            return withValueDeserializerType(castNonGeneric(deserializerType, type));
        }

        /**
         * Override the default {@code auto.offset.reset} policy of {@code earliest}
         *
         * @param earliest if {@code true}, set {@code auto.offset.reset} to {@code earliest},
         *     otherwise {@code latest}.
         * @return self
         */
        public ConsumerBuilder<K, V> withAutoOffsetReset(final boolean earliest) {
            this.autoOffsetResetEarliest = earliest;
            return this;
        }

        /**
         * @return type-safe consumer properties.
         */
        public ConsumerProperties<K, V> buildProperties() {
            final Class<? extends Deserializer<K>> keyDeser =
                    keyDeserializer.orElseThrow(
                            () ->
                                    new ClientsException(
                                            "key deserializer not set. Call either withKeyType or"
                                                    + " withKeyDeserializerType."));
            final Class<? extends Deserializer<V>> valDeser =
                    valDeserializer.orElseThrow(
                            () ->
                                    new ClientsException(
                                            "value deserializer not set. Call either withValueType"
                                                    + " or withValueDeserializerType."));

            final Map<String, Object> props = clientBuilder.baseProps();

            props.putAll(
                    Map.of(
                            ConsumerConfig.CLIENT_ID_CONFIG,
                            clientBuilder.clientIdentifier("consumer"),
                            ConsumerConfig.GROUP_ID_CONFIG,
                            clientBuilder.clientIdentifier("consumer-group"),
                            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                            autoOffsetResetEarliest ? "earliest" : "latest",
                            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                            keyDeser,
                            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                            valDeser));
            props.putAll(
                    clientBuilder.overrides(
                            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
            return new ConsumerProperties<>(props);
        }

        /**
         * @return new Kafka consumer.
         */
        public Consumer<K, V> build() {
            return consumer(buildProperties());
        }

        private static <T> Class<? extends Deserializer<T>> deserializerFor(final Class<T> type) {
            if (MessageLiteOrBuilder.class.isAssignableFrom(type)) {
                return castNonGeneric(KafkaProtobufDeserializer.class, type);
            }

            if (GenericRecord.class.isAssignableFrom(type)) {
                return castNonGeneric(KafkaAvroDeserializer.class, type);
            }

            return stdSerdeType(type).deserializer;
        }

        @SuppressWarnings({"unused", "rawtypes"})
        private static <T> Class<? extends Deserializer<T>> castNonGeneric(
                final Class<? extends Deserializer> deserializerType, final Class<T> type) {
            return (Class<? extends Deserializer<T>>) deserializerType;
        }
    }

    /**
     * Type-safe builder of KStream config
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    @SuppressWarnings({"unchecked", "OptionalUsedAsFieldOrParameterType"})
    public static final class KStreamsBuilder<K, V> {

        private static final Map<String, ?> DEFAULT_STREAM_PROPS = Map.of();

        private final ClientBuilder clientBuilder;
        private Optional<Class<? extends Serde<K>>> keySerde = Optional.empty();
        private Optional<Class<? extends Serde<V>>> valSerde = Optional.empty();
        private boolean acksAll = true;

        private KStreamsBuilder(final ClientBuilder clientBuilder) {
            this.clientBuilder = requireNonNull(clientBuilder, "clientBuilder");
        }

        /**
         * Set the key serde from the supplied {@code type}.
         *
         * <p>Method will attempt to determine the correct serde type from the supplied {@code
         * type}. throwing an exception if there is insufficient type information to this to be
         * determined.
         *
         * <p>The supplied {@code type} can be on of the types supported by the standard Kafka
         * serde, (see {@link #STD_SERDE}), or a generated Protobuf or Avro type.
         *
         * <p>If this method throws due to insufficient type information, use on of the {@link
         * #withKeySerdeType} methods to supply more information.
         *
         * @param type the new key type
         * @return self.
         * @param <T> the new key type
         */
        public <T> KStreamsBuilder<T, V> withKeyType(final Class<T> type) {
            return withKeySerdeType(serdeFor(type));
        }

        /**
         * Set the key serde to the supplied {@code type}.
         *
         * @param type the key serde type
         * @return self.
         * @param <T> the new key type
         */
        public <T> KStreamsBuilder<T, V> withKeySerdeType(final Class<? extends Serde<T>> type) {
            final KStreamsBuilder<T, V> adjusted = (KStreamsBuilder<T, V>) this;
            adjusted.keySerde = Optional.of(type);
            return (KStreamsBuilder<T, V>) this;
        }

        /**
         * Set the key serde to a non-generic type.
         *
         * <p>Some serde, e.g. {@link GenericAvroSerde}, do not accept template parameters that
         * represent the type they operate on. This method variant avoids the need for casts in code
         * by accepting the target type and performing the cast.
         *
         * @param serdeType the serde type
         * @param type the key type the serde handles
         * @return self
         * @param <T> the new key type
         */
        @SuppressWarnings("rawtypes")
        public <T> KStreamsBuilder<T, V> withKeySerdeType(
                final Class<? extends Serde> serdeType, final Class<T> type) {
            return withKeySerdeType(castNonGeneric(serdeType, type));
        }

        /**
         * Set the value serde from the supplied {@code type}.
         *
         * <p>Method will attempt to determine the correct serde type from the supplied {@code
         * type}. throwing an exception if there is insufficient type information to this to be
         * determined.
         *
         * <p>The supplied {@code type} can be on of the types supported by the standard Kafka
         * serde, (see {@link #STD_SERDE}), or a generated Protobuf or Avro type.
         *
         * <p>If this method throws due to insufficient type information, use on of the {@link
         * #withValueSerdeType} methods to supply more information.
         *
         * @param type the new value type
         * @return self.
         * @param <T> the new value type
         */
        public <T> KStreamsBuilder<K, T> withValueType(final Class<T> type) {
            return withValueSerdeType(serdeFor(type));
        }

        /**
         * Set the value serde to the supplied {@code type}.
         *
         * @param type the value serde type
         * @return self.
         * @param <T> the new value type
         */
        public <T> KStreamsBuilder<K, T> withValueSerdeType(final Class<? extends Serde<T>> type) {
            final KStreamsBuilder<K, T> adjusted = (KStreamsBuilder<K, T>) this;
            adjusted.valSerde = Optional.of(type);
            return (KStreamsBuilder<K, T>) this;
        }

        /**
         * Set the value serde to a non-generic type.
         *
         * <p>Some serde, e.g. {@link GenericAvroSerde}, do not accept template parameters that
         * represent the type they operate on. This method variant avoids the need for casts in code
         * by accepting the target type and performing the cast.
         *
         * @param serdeType the serde type
         * @param type the value type the serde handles
         * @return self
         * @param <T> the new value type
         */
        @SuppressWarnings("rawtypes")
        public <T> KStreamsBuilder<K, T> withValueSerdeType(
                final Class<? extends Serde> serdeType, final Class<T> type) {
            return withValueSerdeType(castNonGeneric(serdeType, type));
        }

        /**
         * Override the default acks required of {@code all}.
         *
         * @param acksAll if true, {@code all} acks are required, otherwise {@code 1}.
         * @return self
         */
        public KStreamsBuilder<K, V> withAcks(final boolean acksAll) {
            this.acksAll = acksAll;
            return this;
        }

        /**
         * @return type-safe KStreams properties.
         */
        public KStreamsProperties<K, V> buildProperties() {
            final Class<? extends Serde<K>> keySerdeType =
                    keySerde.orElseThrow(
                            () ->
                                    new ClientsException(
                                            "key serde not set. Call either withKeyType or"
                                                    + " withKeySerdeType."));
            final Class<? extends Serde<V>> valSerdeType =
                    valSerde.orElseThrow(
                            () ->
                                    new ClientsException(
                                            "value serde not set. Call either withSerdeType"
                                                    + " or withValueSerdeType."));

            final Map<String, Object> props = clientBuilder.baseProps();
            props.putAll(DEFAULT_STREAM_PROPS);
            props.putAll(ProducerBuilder.DEFAULT_PRODUCER_PROPS);
            props.putAll(
                    Map.of(
                            StreamsConfig.APPLICATION_ID_CONFIG,
                            clientBuilder.domainId + "._private." + clientBuilder.serviceId,
                            StreamsConfig.CLIENT_ID_CONFIG,
                            clientBuilder.clientIdentifier("client"),
                            StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                            keySerdeType,
                            StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                            valSerdeType,
                            StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,
                            Duration.ofSeconds(10).toMillis(),
                            ProducerConfig.ACKS_CONFIG,
                            acksAll ? "all" : "1"));
            props.putAll(
                    clientBuilder.overrides(
                            StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                            StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG));
            return new KStreamsProperties<>(props);
        }

        private static <T> Class<? extends Serde<T>> serdeFor(final Class<T> type) {
            if (MessageLiteOrBuilder.class.isAssignableFrom(type)) {
                return castNonGeneric(KafkaProtobufSerde.class, type);
            }

            if (GenericRecord.class.isAssignableFrom(type)) {
                return castNonGeneric(GenericAvroSerde.class, type);
            }

            return stdSerdeType(type).serde;
        }

        @SuppressWarnings({"unused", "rawtypes"})
        private static <T> Class<? extends Serde<T>> castNonGeneric(
                final Class<? extends Serde> serdeType, final Class<T> type) {
            return (Class<? extends Serde<T>>) serdeType;
        }
    }

    /**
     * Create a Kafka producer.
     *
     * @param properties the type-safe properties to create it from
     * @return the producer
     * @param <K> the type of the record key
     * @param <V> the type of the record value
     */
    public static <K, V> KafkaProducer<K, V> producer(final ProducerProperties<K, V> properties) {
        return new KafkaProducer<>(properties.asMap());
    }

    /**
     * Create a Kafka consumer.
     *
     * @param properties the type-safe properties to create it from
     * @return the consumer
     * @param <K> the type of the record key
     * @param <V> the type of the record value
     */
    public static <K, V> KafkaConsumer<K, V> consumer(final ConsumerProperties<K, V> properties) {
        return new KafkaConsumer<>(properties.asMap());
    }

    /**
     * Create a Kafka producer
     *
     * @param keyClass the type of the key
     * @param valueClass the type of the value
     * @param producerProperties the properties
     * @param <K> the type of the key
     * @param <V> the type of the value
     * @return the producer
     * @deprecated use the type-safe {@link ClientBuilder#producer()} or {@link
     *     #producer(ProducerProperties)}.
     */
    @SuppressWarnings("unused")
    @Deprecated(forRemoval = true, since = "0.15.1")
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
     * @deprecated use the type-safe {@link ClientBuilder#producer()} or {@link
     *     #producer(ProducerProperties)}.§§
     */
    @SuppressWarnings({"checkstyle:ParameterNumber", "DeprecatedIsStillUsed"})
    @SafeVarargs
    @Deprecated(forRemoval = true, since = "0.15.1")
    public static Map<String, Object> producerProperties(
            final String domainId,
            final String serviceId,
            final String bootstrapServers,
            final String schemaRegistryUrl,
            final Class<?> keySerializerClass,
            final Class<?> valueSerializerClass,
            final boolean acksAll,
            final Map<String, Object>... additionalProperties) {

        ClientBuilder builder =
                Clients.builder(domainId, serviceId, bootstrapServers, schemaRegistryUrl);

        for (final Map<String, Object> additional : additionalProperties) {
            builder = builder.withProps(additional);
        }

        final ProducerProperties<Void, Void> producerProps =
                builder.producer()
                        .withKeyType(Void.class)
                        .withValueType(Void.class)
                        .withAcks(acksAll)
                        .buildProperties();

        final Map<String, Object> props = new HashMap<>(producerProps.asMap());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass);
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
     * @deprecated use the type-safe {@link ClientBuilder#kstreams()}.
     */
    @SuppressWarnings({"checkstyle:ParameterNumber", "DeprecatedIsStillUsed"})
    @SafeVarargs
    @Deprecated(forRemoval = true, since = "0.15.1")
    public static Map<String, Object> kstreamsProperties(
            final String domainId,
            final String serviceId,
            final String bootstrapServers,
            final String schemaRegistryUrl,
            final Class<?> keySerdeClass,
            final Class<?> valueSerdeClass,
            final boolean acksAll,
            final Map<String, Object>... additionalProperties) {

        ClientBuilder builder =
                Clients.builder(domainId, serviceId, bootstrapServers, schemaRegistryUrl);

        for (final Map<String, Object> additional : additionalProperties) {
            builder = builder.withProps(additional);
        }

        final KStreamsProperties<Void, Void> kstreamProps =
                builder.kstreams()
                        .withKeyType(Void.class)
                        .withValueType(Void.class)
                        .withAcks(acksAll)
                        .buildProperties();

        final Map<String, Object> props = new HashMap<>(kstreamProps.asMap());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, keySerdeClass);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, valueSerdeClass);
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
     * @return the consumer
     * @deprecated use the type-safe {@link ClientBuilder#consumer()} or {@link
     *     #consumer(ConsumerProperties)}.
     */
    @SuppressWarnings("unused")
    @Deprecated(forRemoval = true, since = "0.15.1")
    public static <K, V> KafkaConsumer<K, V> consumer(
            final Class<K> keyClass,
            final Class<V> valueClass,
            final Map<String, Object> consumerProperties) {
        return new KafkaConsumer<>(consumerProperties);
    }

    /**
     * Create a map of consumer properties with sensible defaults.
     *
     * @param domainId the domain id of the consumer, used to scope resource names.
     * @param serviceId the name of the service
     * @param bootstrapServers bootstrap servers config
     * @param schemaRegistryUrl url of schema registry
     * @param keyDeserializerClass type of key deserializer
     * @param valueDeserializerClass type of value deserializer
     * @param autoOffsetResetEarliest reset to earliest offset if no stored offsets?
     * @param additionalProperties additional properties
     * @return props
     * @deprecated @deprecated use the type-safe {@link ClientBuilder#consumer()}.
     */
    @Deprecated(forRemoval = true, since = "0.15.1")
    @SuppressWarnings({"checkstyle:ParameterNumber", "DeprecatedIsStillUsed"})
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

        ClientBuilder builder =
                Clients.builder(domainId, serviceId, bootstrapServers, schemaRegistryUrl);

        for (final Map<String, Object> additional : additionalProperties) {
            builder = builder.withProps(additional);
        }

        final ConsumerProperties<Void, Void> consumerProps =
                builder.consumer()
                        .withKeyType(Void.class)
                        .withValueType(Void.class)
                        .withAutoOffsetReset(autoOffsetResetEarliest)
                        .buildProperties();

        final Map<String, Object> props = new HashMap<>(consumerProps.asMap());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass);
        return props;
    }

    @SuppressWarnings("unchecked")
    private static <T> SerdeTypes<T> stdSerdeType(final Class<T> type) {
        final SerdeTypes<T> found = (SerdeTypes<T>) STD_SERDE.get(type);
        if (found != null) {
            return found;
        }

        throw new ClientsException("Could not determine serde for type: " + type.getName());
    }

    private static <T> SerdeTypes<T> typeMetaData(
            final Class<? extends Serializer<T>> serializer,
            final Class<? extends Deserializer<T>> deserializer,
            final Class<? extends Serde<T>> serde) {
        return new SerdeTypes<>(serializer, deserializer, serde);
    }

    private static final class SerdeTypes<T> {

        private final Class<? extends Serializer<T>> serializer;
        private final Class<? extends Deserializer<T>> deserializer;
        private final Class<? extends Serde<T>> serde;

        SerdeTypes(
                final Class<? extends Serializer<T>> serializer,
                final Class<? extends Deserializer<T>> deserializer,
                final Class<? extends Serde<T>> serde) {
            this.serializer = requireNonNull(serializer, "serializer");
            this.deserializer = requireNonNull(deserializer, "deserializer");
            this.serde = requireNonNull(serde, "serde");
        }
    }

    private static class ClientsException extends RuntimeException {
        ClientsException(final String message) {
            super(message);
        }

        ClientsException(final String message, final Exception cause) {
            super(message, cause);
        }
    }
}
