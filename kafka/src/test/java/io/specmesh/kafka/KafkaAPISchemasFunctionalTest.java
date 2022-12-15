package io.specmesh.kafka;


import static io.specmesh.kafka.Clients.consumer;
import static io.specmesh.kafka.Clients.consumerProperties;
import static io.specmesh.kafka.Clients.producer;
import static io.specmesh.kafka.Clients.producerProperties;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.specmesh.apiparser.AsyncApiParser;
import io.specmesh.apiparser.model.ApiSpec;
import io.specmesh.kafka.schema.SchemaRegistryContainer;
import io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import simple.schema_demo._public.user_signed_up_value.UserSignedUp;


@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class KafkaAPISchemasFunctionalTest {
    public static final int WAIT = 10;
    private static final String CFLT_VERSION = "6.2.7";

    private static final Network network = Network.newNetwork();

    private static KafkaContainer kafkaContainer;
    private static SchemaRegistryContainer schemaRegistryContainer;


    private static final KafkaApiSpec apiSpec = new KafkaApiSpec(getAPISpecFromResource());

    private AdminClient adminClient;
    private SchemaRegistryClient schemaRegistryClient;

    @BeforeEach
    public void createAllTheThings() {
        System.out.println("createAllTheThings BROKER URL:" + kafkaContainer.getBootstrapServers());
        adminClient = AdminClient.create(getClientProperties());
        schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryContainer.getUrl(), 1000);
    }


    @Order(1)
    @Test
    void shouldProduceAndConsumeUsingAvro() throws Exception {
        final List<NewTopic> domainTopics = apiSpec.listDomainOwnedTopics();
        adminClient.createTopics(domainTopics).all().get(WAIT, TimeUnit.SECONDS);
        System.out.println("CREATED TOPICS: " + domainTopics.size());
        final var schemaInfo = apiSpec.schemaInfoForTopic(domainTopics.get(0).name());

        final var topicSubject = "simple.schema_demo._public.user_signed_up";
        final var schemaRef = schemaInfo.schemaRef();
        assertThat(schemaRef, is("/schema/" + topicSubject + ".avsc"));

        final var schemaContent = Files.readString(
                Path.of(
                        Objects.requireNonNull(getClass()
                                        .getResource(schemaRef))
                                .toURI()),
                UTF_8);
        final AvroSchema avroSchema = new AvroSchema(schemaContent);

        // register the schema against the topic (subject)
        schemaRegistryClient.register(topicSubject + "-value", avroSchema);
        final KafkaProducer<Long, UserSignedUp> domainProducer = getDomainProducer(apiSpec.id());
        final var sentRecord = new UserSignedUp("joe blogs", "blogy@twasmail.com", 100);
        domainProducer.send(
                new ProducerRecord<>(domainTopics.get(0).name(), 1000L,
                        sentRecord
                )
        ).get();


        final KafkaConsumer<Long, UserSignedUp> domainConsumer = getDomainConsumer(apiSpec.id());
        domainConsumer.subscribe(Collections.singleton(domainTopics.get(0).name()));
        final ConsumerRecords<Long, UserSignedUp> consumerRecords = domainConsumer.poll(Duration.ofSeconds(10));
        assertThat(consumerRecords, is(notNullValue()));
        assertThat(consumerRecords.iterator().next().value(), is(sentRecord));
    }


    @Order(2)
    @Test
    void shouldProvisionProduceAndConsumeUsingAvroWithSpeccy() throws Exception {

        Provisioner.provisionTopicsAndSchemas(apiSpec, adminClient, schemaRegistryClient, "./build/resources/test");

        final List<NewTopic> domainTopics = apiSpec.listDomainOwnedTopics();
        final var userSignedUpTopic = domainTopics.stream()
                .filter(topic -> topic.name().endsWith("_public.user_signed_up")).findFirst().get().name();

        /*
          Produce on the schema
         */
        final KafkaProducer<Long, UserSignedUp> domainProducer = producer(
                Long.class,
                UserSignedUp.class,
                producerProperties(
                        apiSpec.id(),
                        kafkaContainer.getBootstrapServers(),
                        schemaRegistryContainer.getUrl(),
                        LongSerializer.class,
                        KafkaAvroSerializer.class,
                        Map.of()));
        final var sentRecord = new UserSignedUp("joe blogs" , "blogy@twasmail.com", 100);

        domainProducer.send(
                new ProducerRecord<>(userSignedUpTopic, 1000L,
                        sentRecord
                )
        ).get();


        final KafkaConsumer<Long, UserSignedUp> consumer = consumer(
                Long.class,
                UserSignedUp.class,
                consumerProperties(
                        apiSpec.id()+"222",
                    kafkaContainer.getBootstrapServers(),
                    schemaRegistryContainer.getUrl(),
                    LongDeserializer.class,
                    KafkaAvroDeserializer.class,
                true,
                    Map.of()));
        consumer.subscribe(Collections.singleton(userSignedUpTopic));
        final ConsumerRecords<Long, UserSignedUp> consumerRecords = consumer.poll(Duration.ofSeconds(10));
        assertThat(consumerRecords, is(notNullValue()));
        assertThat(consumerRecords.count(), is(1));
        assertThat(consumerRecords.iterator().next().value(), is(sentRecord));
    }

    @Order(3)
    @Test
    void shouldProvisionProduceAndConsumeProtoWithSpeccy() throws Exception {

        Provisioner.provisionTopicsAndSchemas(apiSpec, adminClient, schemaRegistryClient, "./build/resources/test");

        final List<NewTopic> domainTopics = apiSpec.listDomainOwnedTopics();
        final var userInfoTopic = domainTopics.stream().filter(topic -> topic.name().endsWith("_public.user_info")).findFirst().get().name();

        /*
          Produce on the schema
         */
        final KafkaProducer<Long, UserInfo> domainProducer = producer(
                        Long.class,
                        UserInfo.class,
                        producerProperties(
                                apiSpec.id(), kafkaContainer.getBootstrapServers(),
                            schemaRegistryContainer.getUrl(),
                            LongSerializer.class,
                            KafkaProtobufSerializer.class,
                            Map.of()
                        )
                );
        final var userSam = UserInfo.newBuilder().setFullName("sam fteex").setEmail("hello-sam@bahamas.island").setAge(52).build();


        domainProducer.send(
                new ProducerRecord<>(userInfoTopic, 1000L,
                        userSam
                )
        ).get();


        final KafkaConsumer<Long, UserInfo> consumer = consumer(Long.class, UserInfo.class,
                consumerProperties(
                        apiSpec.id()+"3",
                        kafkaContainer.getBootstrapServers(),
                        schemaRegistryContainer.getUrl(),
                        LongDeserializer.class,
                        KafkaProtobufDeserializer.class,
                        true,
                        Map.of(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, UserInfo.class.getName() ))
        );
        consumer.subscribe(Collections.singleton(userInfoTopic));
        final ConsumerRecords<Long, UserInfo> consumerRecords = consumer.poll(Duration.ofSeconds(10));
        assertThat(consumerRecords, is(notNullValue()));
        assertThat(consumerRecords.count(), is(1));
        assertThat(consumerRecords.iterator().next().value(), is(userSam));
    }


    @SuppressWarnings("unchecked")
    private Map<String, Object> cloneProperties(final Properties adminClientProperties, final Map<String, String> entries) {
        Map<String, Object> mutableMap = new HashMap<String, Object>((Map) adminClientProperties);
        mutableMap.putAll(entries);
        return mutableMap;
    }

    private static ApiSpec getAPISpecFromResource() {
        try {
            return new AsyncApiParser().loadResource(KafkaAPISchemasFunctionalTest.class.getClassLoader()
                    .getResourceAsStream("simple_schema_demo-api.yaml"));
        } catch (Throwable t) {
            throw new RuntimeException("Failed to load test resource", t);
        }
    }

    private KafkaProducer<Long, UserSignedUp> getDomainProducer(final String domainId) {
        return new KafkaProducer<>(
                cloneProperties(getClientProperties(),
                        Map.of(
                                AdminClientConfig.CLIENT_ID_CONFIG, domainId + ".producer",
                                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName(),
                                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName(),
                                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                                schemaRegistryContainer.getUrl(),
                                // AUTO-REG should be false to allow schemas to be published by controlled processes
                                AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "false",
                                // schema-reflect MUST be true when writing Java objects (otherwise you send a datum-container instead of a Pogo)
                                KafkaAvroSerializerConfig.SCHEMA_REFLECTION_CONFIG, "true",
                                KafkaAvroSerializerConfig.USE_LATEST_VERSION, "true"
                        )
                ));
    }

    private KafkaConsumer<Long, UserSignedUp> getDomainConsumer(final String domainId) {
        return new KafkaConsumer<>(
                cloneProperties(getClientProperties(),
                        Map.of(
                                ConsumerConfig.CLIENT_ID_CONFIG, domainId + ".consumer",
                                ConsumerConfig.GROUP_ID_CONFIG, domainId + ".consumer-group",
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getCanonicalName(),
                                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getCanonicalName(),
                                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryContainer.getUrl(),
                                AbstractKafkaSchemaSerDeConfig.SCHEMA_REFLECTION_CONFIG, "true"
                        )
                ));
    }

    private static Properties getClientProperties() {
        final Properties adminClientProperties = new Properties();
        adminClientProperties.put(AdminClientConfig.CLIENT_ID_CONFIG, apiSpec.id());
        adminClientProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        return adminClientProperties;
    }


    @AfterAll public static void stopAll() {
        kafkaContainer.stop();
        schemaRegistryContainer.stop();
    }
    @BeforeAll
    public static void startContainers() {

        try {
            kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:" + CFLT_VERSION))
                    .withNetwork(network)
                    .withStartupTimeout(Duration.ofSeconds(90));

            schemaRegistryContainer = new SchemaRegistryContainer(CFLT_VERSION)
                    .withNetwork(network)
                    .withKafka(kafkaContainer)
                    .withStartupTimeout(Duration.ofSeconds(90))
                ;

            Startables
                    .deepStart(Stream.of(kafkaContainer, schemaRegistryContainer))
                    .join();

        } catch (Throwable t) {
            t.printStackTrace();
        }

    }
}
