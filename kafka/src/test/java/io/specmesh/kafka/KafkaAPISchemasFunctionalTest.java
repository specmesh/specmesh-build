package io.specmesh.kafka;


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
import io.specmesh.apiparser.AsyncApiParser;
import io.specmesh.apiparser.model.ApiSpec;
import io.specmesh.kafka.schema.SchemaRegistryContainer;
import io.specmesh.kafka.schema.SimpleSchemaDemoPublicUserInfo.UserInfo;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
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
    void shouldProvisionProduceAndConsumeUsingAvroWithSpeccy() throws Exception {

        Provisioner.provisionTopicsAndSchemas(apiSpec, adminClient, schemaRegistryClient, "./build/resources/test");

        final List<NewTopic> domainTopics = apiSpec.listDomainOwnedTopics();
        final var userSignedUpTopic = domainTopics.stream()
                .filter(topic -> topic.name().endsWith("_public.user_signed_up")).findFirst().get().name();

        /*
          Produce on the schema
         */
        final KafkaProducer<Long, UserSignedUp> domainProducer = Clients.producer(
                Long.class,
                UserSignedUp.class,
                Clients.producerProperties(
                        apiSpec.id(),
                        "do-things",
                        kafkaContainer.getBootstrapServers(),
                        schemaRegistryContainer.getUrl(),
                        LongSerializer.class,
                        KafkaAvroSerializer.class,
                        false,
                        Map.of()));
        final var sentRecord = new UserSignedUp("joe blogs" , "blogy@twasmail.com", 100);


        domainProducer.send(
                new ProducerRecord<>(userSignedUpTopic,
                        1000L,
                        sentRecord
                )
        ).get();


        final KafkaConsumer<Long, UserSignedUp> consumer = Clients.consumer(
                Long.class,
                UserSignedUp.class,
                Clients.consumerProperties(
                        apiSpec.id(),
                        "do-things-in",
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

    @Order(2)
    @Test
    void shouldProvisionProduceAndConsumeProtoWithSpeccy() throws Exception {

        Provisioner.provisionTopicsAndSchemas(apiSpec, adminClient, schemaRegistryClient, "./build/resources/test");

        final List<NewTopic> domainTopics = apiSpec.listDomainOwnedTopics();
        final var userInfoTopic = domainTopics.stream().filter(topic -> topic.name().endsWith("_public.user_info")).findFirst().get().name();

        /*
          Produce on the schema
         */
        final KafkaProducer<Long, UserInfo> domainProducer = Clients.producer(
                        Long.class,
                        UserInfo.class,
                        Clients.producerProperties(
                                apiSpec.id(),
                                "do-things-user-info",
                                kafkaContainer.getBootstrapServers(),
                                schemaRegistryContainer.getUrl(),
                                LongSerializer.class,
                                KafkaProtobufSerializer.class,
                                false,
                                Map.of()
                        )
                );
        final var userSam = UserInfo.newBuilder().setFullName("sam fteex").setEmail("hello-sam@bahamas.island").setAge(52).build();


        domainProducer.send(
                new ProducerRecord<>(userInfoTopic, 1000L,
                        userSam
                )
        ).get();


        final KafkaConsumer<Long, UserInfo> consumer = Clients.consumer(Long.class, UserInfo.class,
                Clients.consumerProperties(
                        apiSpec.id(),
                        "do-things-user-info-in",
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


    private static ApiSpec getAPISpecFromResource() {
        try {
            return new AsyncApiParser().loadResource(KafkaAPISchemasFunctionalTest.class.getClassLoader()
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
                    .withStartupTimeout(Duration.ofSeconds(90));

            Startables
                    .deepStart(Stream.of(kafkaContainer, schemaRegistryContainer))
                    .join();

        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}
