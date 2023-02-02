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

// CHECKSTYLE_RULES.OFF: FinalLocalVariable
// CHECKSTYLE_RULES.OFF: FinalParameters
package io.specmesh.kafka;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.Sets;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class SASLPlainPrincipleTest {

    private static final String DOMAIN_ROOT = "simple.streetlights";
    private static final String FOREIGN_DOMAIN = "london.hammersmith.transport";

    private static final String PUBLIC_LIGHT_MEASURED = ".public.light.measured";
    private static final String PRIVATE_LIGHT_EVENTS = ".private.light.events";

    private static final String ADMIN_USER = "admin";
    private static final String PRODUCER_USER = DOMAIN_ROOT + "_producer";
    private static final String CONSUMER_USER = DOMAIN_ROOT + "_consumer";
    private static final String FOREIGN_CONSUMER_USER = FOREIGN_DOMAIN + "_consumer";

    @RegisterExtension
    private static final KafkaEnvironment KAFKA_ENV =
            DockerKafkaEnvironment.builder()
                    .withoutSchemaRegistry()
                    .withSaslAuthentication(
                            ADMIN_USER,
                            ADMIN_USER + "-secret",
                            PRODUCER_USER,
                            PRODUCER_USER + "-secret",
                            CONSUMER_USER,
                            CONSUMER_USER + "-secret",
                            FOREIGN_CONSUMER_USER,
                            FOREIGN_CONSUMER_USER + "-secret")
                    .withKafkaAcls()
                    .build();

    private Admin adminClient;
    private Producer<Long, String> domainProducer;
    private Consumer<Long, String> domainConsumer;
    private Consumer<Long, String> foreignConsumer;

    @BeforeEach
    public void createAllTheThings() {

        final Properties adminClientProperties = new Properties();
        adminClientProperties.put(AdminClientConfig.CLIENT_ID_CONFIG, DOMAIN_ROOT);
        adminClientProperties.put(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_ENV.kafkaBootstrapServers());
        adminClientProperties.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        adminClientProperties.put("sasl.mechanism", "PLAIN");
        adminClientProperties.put(
                "sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                        + "   username=\""
                        + ADMIN_USER
                        + "\" password=\""
                        + ADMIN_USER
                        + "-secret\";");

        adminClient = AdminClient.create(adminClientProperties);
        CreateTopicsResult topics =
                adminClient.createTopics(
                        Sets.newHashSet(
                                new NewTopic(
                                        DOMAIN_ROOT + PUBLIC_LIGHT_MEASURED,
                                        1,
                                        Short.parseShort("1")),
                                new NewTopic(
                                        DOMAIN_ROOT + PRIVATE_LIGHT_EVENTS,
                                        1,
                                        Short.parseShort("1")),
                                new NewTopic(
                                        ".london.hammersmith.transport.public.tube",
                                        1,
                                        Short.parseShort("1"))));

        topics.values()
                .values()
                .forEach(
                        f -> {
                            try {
                                f.get();
                            } catch (ExecutionException
                                    | InterruptedException
                                    | RuntimeException e) {
                                throw new RuntimeException(e);
                            }
                        });

        domainProducer =
                new KafkaProducer<>(
                        cloneProperties(
                                adminClientProperties,
                                Map.of(
                                        AdminClientConfig.CLIENT_ID_CONFIG,
                                        DOMAIN_ROOT + ".producer",
                                        "sasl.jaas.config",
                                        String.format(
                                                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                                                        + "   username=\"%s\" "
                                                        + "   password=\"%s-secret\";",
                                                PRODUCER_USER, PRODUCER_USER))),
                        Serdes.Long().serializer(),
                        Serdes.String().serializer());

        domainConsumer =
                new KafkaConsumer<>(
                        cloneProperties(
                                adminClientProperties,
                                Map.of(
                                        ConsumerConfig.CLIENT_ID_CONFIG,
                                        DOMAIN_ROOT + ".consumer",
                                        ConsumerConfig.GROUP_ID_CONFIG,
                                        DOMAIN_ROOT + ".consumer-group",
                                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                                        "earliest",
                                        "sasl.jaas.config",
                                        String.format(
                                                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                                                        + "   username=\"%s\" "
                                                        + "   password=\"%s-secret\";",
                                                CONSUMER_USER, CONSUMER_USER))),
                        Serdes.Long().deserializer(),
                        Serdes.String().deserializer());

        foreignConsumer =
                new KafkaConsumer<>(
                        cloneProperties(
                                adminClientProperties,
                                Map.of(
                                        ConsumerConfig.CLIENT_ID_CONFIG,
                                        FOREIGN_DOMAIN + ".consumer",
                                        ConsumerConfig.GROUP_ID_CONFIG,
                                        FOREIGN_DOMAIN + ".consumer-group",
                                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                                        "earliest",
                                        "sasl.jaas.config",
                                        String.format(
                                                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                                                        + "   username=\"%s\" "
                                                        + "   password=\"%s-secret\";",
                                                FOREIGN_CONSUMER_USER, FOREIGN_CONSUMER_USER))),
                        Serdes.Long().deserializer(),
                        Serdes.String().deserializer());
    }

    @Test
    public void shouldPubSubStuff() throws Exception {

        domainProducer
                .send(new ProducerRecord<>(DOMAIN_ROOT + PUBLIC_LIGHT_MEASURED, 100L, "got value"))
                .get();

        domainConsumer.subscribe(Collections.singleton(DOMAIN_ROOT + PUBLIC_LIGHT_MEASURED));
        ConsumerRecords<Long, String> poll =
                domainConsumer.poll(Duration.of(30, TimeUnit.SECONDS.toChronoUnit()));

        assertThat("Didnt get Record", poll.count(), is(1));

        foreignConsumer.subscribe(Collections.singleton(DOMAIN_ROOT + PUBLIC_LIGHT_MEASURED));
        ConsumerRecords<Long, String> pollForeign =
                foreignConsumer.poll(Duration.of(30, TimeUnit.SECONDS.toChronoUnit()));

        assertThat("Didnt get Record", pollForeign.count(), is(1));
    }

    private Properties cloneProperties(
            Properties adminClientProperties, Map<String, String> entries) {
        Properties results = new Properties();
        results.putAll(adminClientProperties);
        results.putAll(entries);
        return results;
    }
}
