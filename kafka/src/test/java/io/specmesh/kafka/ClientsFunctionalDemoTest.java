package io.specmesh.kafka;

import static io.specmesh.kafka.Clients.consumer;
import static io.specmesh.kafka.Clients.consumerProperties;
import static io.specmesh.kafka.Clients.producer;
import static io.specmesh.kafka.Clients.producerProperties;
import static org.apache.kafka.streams.kstream.Produced.with;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

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
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.commons.collections.MapUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import simple.schema_demo._public.user_signed_up_value.UserSignedUp;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ClientsFunctionalDemoTest extends AbstractContainerTest {
    private static final KafkaApiSpec apiSpec = new KafkaApiSpec(getAPISpecFromResource());
    private AdminClient adminClient;
    private SchemaRegistryClient schemaRegistryClient;

    @BeforeEach
    public void createAllTheThings() {
        adminClient = AdminClient.create(getClientProperties());
        schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryContainer.getUrl(), 1000);
    }

    @Order(1)
    @Test
    void shouldProvisionProduceAndConsumeUsingAvroWithSpeccy() throws Exception {
        Provisioner.provisionTopics(adminClient, apiSpec);
        Provisioner.provisionSchemas(apiSpec, schemaRegistryClient, "./build/resources/test");

        final var domainTopics = apiSpec.listDomainOwnedTopics();
        final var userSignedUpTopic = domainTopics.stream()
                .filter(topic -> topic.name().endsWith("_public.user_signed_up")).findFirst()
                .orElseThrow(() -> new RuntimeException("user_signed_up topic not found")).name();

        /*
         * Produce on the schema
         */
        final KafkaProducer<Long, UserSignedUp> producer = producer(Long.class, UserSignedUp.class,
                producerProperties(apiSpec.id(), "do-things", kafkaContainer.getBootstrapServers(),
                        schemaRegistryContainer.getUrl(), LongSerializer.class, KafkaAvroSerializer.class, false,
                        Map.of()));
        final var sentRecord = new UserSignedUp("joe blogs", "blogy@twasmail.com", 100);

        producer.send(new ProducerRecord<>(userSignedUpTopic, 1000L, sentRecord)).get(60, TimeUnit.SECONDS);

        final KafkaConsumer<Long, UserSignedUp> consumer = consumer(Long.class, UserSignedUp.class,
                consumerProperties(apiSpec.id(), "do-things-in", kafkaContainer.getBootstrapServers(),
                        schemaRegistryContainer.getUrl(), LongDeserializer.class, KafkaAvroDeserializer.class, true,
                        Map.of()));
        consumer.subscribe(Collections.singleton(userSignedUpTopic));

        final ConsumerRecords<Long, UserSignedUp> consumerRecords = consumer.poll(Duration.ofSeconds(10));
        assertThat(consumerRecords, is(notNullValue()));
        assertThat(consumerRecords.count(), is(1));
        assertThat(consumerRecords.iterator().next().value(), is(sentRecord));
    }

    @Order(2)
    @Test
    void shouldProvisionProduceAndConsumeProtoWithSpeccyClient() throws Exception {

        Provisioner.provisionTopics(adminClient, apiSpec);
        Provisioner.provisionSchemas(apiSpec, schemaRegistryClient, "./build/resources/test");

        final List<NewTopic> domainTopics = apiSpec.listDomainOwnedTopics();
        final var userInfoTopic = domainTopics.stream().filter(topic -> topic.name().endsWith("_public.user_info"))
                .findFirst().orElseThrow().name();

        /*
         * Produce on the schema
         */
        final KafkaProducer<Long, UserInfo> producer = producer(Long.class, UserInfo.class,
                producerProperties(apiSpec.id(), "do-things-user-info", kafkaContainer.getBootstrapServers(),
                        schemaRegistryContainer.getUrl(), LongSerializer.class, KafkaProtobufSerializer.class, false,
                        Map.of()));
        final var userSam = UserInfo.newBuilder().setFullName("sam fteex").setEmail("hello-sam@bahamas.island")
                .setAge(52).build();

        producer.send(new ProducerRecord<>(userInfoTopic, 1000L, userSam)).get(60, TimeUnit.SECONDS);

        final KafkaConsumer<Long, UserInfo> consumer = consumer(Long.class, UserInfo.class, consumerProperties(
                apiSpec.id(), "do-things-user-info-in", kafkaContainer.getBootstrapServers(),
                schemaRegistryContainer.getUrl(), LongDeserializer.class, KafkaProtobufDeserializer.class, true,
                Map.of(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, UserInfo.class.getName())));
        consumer.subscribe(Collections.singleton(userInfoTopic));
        final ConsumerRecords<Long, UserInfo> consumerRecords = consumer.poll(Duration.ofSeconds(10));
        assertThat(consumerRecords, is(notNullValue()));
        assertThat(consumerRecords.count(), is(1));
        assertThat(consumerRecords.iterator().next().value(), is(userSam));
    }

    @Order(3)
    @Test
    void shouldProvisionInfraAndStreamStuffUsingProtoAndSpeccyClient() throws Exception {

        Provisioner.provisionTopics(adminClient, apiSpec);
        Provisioner.provisionSchemas(apiSpec, schemaRegistryClient, "./build/resources/test");

        final var domainTopics = apiSpec.listDomainOwnedTopics();
        final var userInfoTopic = domainTopics.stream().filter(topic -> topic.name().endsWith("_public.user_info"))
                .findFirst().orElseThrow().name();
        final var userInfoEnrichedTopic = domainTopics.stream()
                .filter(topic -> topic.name().endsWith("_public.user_info_enriched")).findFirst().orElseThrow().name();

        final var streamsConfiguration = Clients.kstreamsProperties(apiSpec.id(), "streams-appid-service-thing",
                kafkaContainer.getBootstrapServers(), schemaRegistryContainer.getUrl(), Serdes.LongSerde.class,
                KafkaProtobufSerde.class, false,
                Map.of(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, UserInfo.class.getName()));

        final var builder = new StreamsBuilder();

        final KStream<Long, UserInfo> userInfos = builder.stream(userInfoTopic);

        final var enrichedUsersStream = userInfos.mapValues(userInfo -> UserInfoEnriched.newBuilder()
                .setAddress("hiding in the bahamas").setAge(userInfo.getAge()).setEmail(userInfo.getEmail()).build());

        enrichedUsersStream.to(userInfoEnrichedTopic,
                with(Serdes.Long(), new KafkaProtobufSerde(schemaRegistryClient, UserInfoEnriched.class)));

        final var streams = new KafkaStreams(builder.build(), MapUtils.toProperties(streamsConfiguration));
        streams.start();

        /*
         * Run it
         */
        final KafkaProducer<Long, UserInfo> producer = producer(Long.class, UserInfo.class,
                producerProperties(apiSpec.id(), "do-things-user-info", kafkaContainer.getBootstrapServers(),
                        schemaRegistryContainer.getUrl(), LongSerializer.class, KafkaProtobufSerializer.class, false,
                        Map.of()));

        final var userSam = UserInfo.newBuilder().setFullName("sam fteex").setEmail("hello-sam@bahamas.island")
                .setAge(52).build();
        producer.send(new ProducerRecord<>(userInfoTopic, 1000L, userSam)).get(60, TimeUnit.SECONDS);

        final KafkaConsumer<Long, UserInfoEnriched> consumer = consumer(Long.class, UserInfoEnriched.class,
                consumerProperties(apiSpec.id(), "streams-consumer-validate", kafkaContainer.getBootstrapServers(),
                        schemaRegistryContainer.getUrl(), LongDeserializer.class, KafkaProtobufDeserializer.class, true,
                        Map.of(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE,
                                UserInfoEnriched.class.getName())));
        consumer.subscribe(Collections.singleton(userInfoEnrichedTopic));
        final ConsumerRecords<Long, UserInfoEnriched> consumerRecords = consumer.poll(Duration.ofSeconds(10));

        final var consumerRecordStream = Stream.generate(consumerRecords.iterator()::next);

        /*
         * Verify
         */
        assertThat(consumerRecords, is(notNullValue()));
        final var foundIt = consumerRecordStream
                .filter((record) -> record.value().getAddress().equals("hiding in the bahamas")).findFirst();
        assertThat(foundIt.isPresent(), is(true));
    }

    private static ApiSpec getAPISpecFromResource() {
        try {
            return new AsyncApiParser().loadResource(ClientsFunctionalDemoTest.class.getClassLoader()
                    .getResourceAsStream("simple_schema_demo-api.yaml"));
        } catch (Throwable t) {
            throw new RuntimeException("Failed to load test resource", t);
        }
    }

    private static Properties getClientProperties() {
        final Properties adminClientProperties = new Properties();
        adminClientProperties.put(AdminClientConfig.CLIENT_ID_CONFIG, apiSpec.id());
        adminClientProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        return adminClientProperties;
    }
}
