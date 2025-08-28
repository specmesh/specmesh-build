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

package io.specmesh.kafka.provision;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import io.specmesh.kafka.DockerKafkaEnvironment;
import io.specmesh.kafka.KafkaApiSpec;
import io.specmesh.kafka.KafkaEnvironment;
import io.specmesh.kafka.provision.schema.SchemaProvisioner;
import io.specmesh.kafka.provision.schema.SchemaProvisioner.Schema;
import io.specmesh.test.TestSpecLoader;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Provision schemas with references - missing refs will fail. For common schema set the
 * schemaLookupStrategy: "RecordNameStrategy" so Trade can reference the subject like that
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag("ContainerisedTest")
class SchemaProvisionerReferenceTest {

    private static final KafkaApiSpec COMMON_API_SPEC =
            TestSpecLoader.loadFromClassPath("schema-ref/com.example.shared-api.yml");

    private static final KafkaApiSpec API_SPEC =
            TestSpecLoader.loadFromClassPath("schema-ref/com.example.trading-api.yml");

    private static final String ADMIN_USER = "admin";

    @RegisterExtension
    private static final KafkaEnvironment KAFKA_ENV =
            DockerKafkaEnvironment.builder()
                    .withSaslAuthentication(
                            ADMIN_USER,
                            ADMIN_USER + "-secret",
                            API_SPEC.id(),
                            API_SPEC.id() + "-secret")
                    .build();

    @Test
    @Order(1)
    void shouldFailToProvisionIfSharedSchemaAreNotRegistered() {
        // When:
        final List<Schema> provision =
                SchemaProvisioner.provision(
                        false,
                        false,
                        API_SPEC,
                        "./src/test/resources/schema-ref",
                        KAFKA_ENV.srClient());

        // Then:
        assertThat(provision, hasSize(3));

        final Map<String, Schema> bySubject =
                provision.stream().collect(Collectors.toMap(Schema::subject, Function.identity()));
        assertThat(
                bySubject.keySet(),
                is(
                        Set.of(
                                "com.example.shared.Currency",
                                "com.example.trading._public.trade-value",
                                "com.example.trading.TradeInfo")));

        final Schema commonSchema = bySubject.get("com.example.shared.Currency");
        assertThat(commonSchema.state(), is(Status.STATE.IGNORED));
        assertThat(
                commonSchema.messages(), endsWith("ignored as it does not belong to the domain"));

        final Schema infoSchema = bySubject.get("com.example.trading.TradeInfo");
        assertThat(infoSchema.state(), is(Status.STATE.CREATED));

        final Schema tradeSchema = bySubject.get("com.example.trading._public.trade-value");
        assertThat(tradeSchema.state(), is(Status.STATE.FAILED));
    }

    @Test
    @Order(2)
    void shouldProvisionOnceSharedSchemaAreRegistered() {
        // Given:
        SchemaProvisioner.provision(
                false,
                false,
                COMMON_API_SPEC,
                "./src/test/resources/schema-ref",
                KAFKA_ENV.srClient());

        // When:
        final List<Schema> provision =
                SchemaProvisioner.provision(
                        false,
                        false,
                        API_SPEC,
                        "./src/test/resources/schema-ref",
                        KAFKA_ENV.srClient());

        // Then:
        final Map<String, Schema> bySubject =
                provision.stream().collect(Collectors.toMap(Schema::subject, Function.identity()));
        assertThat(
                bySubject.keySet(),
                is(
                        Set.of(
                                "com.example.shared.Currency",
                                "com.example.trading._public.trade-value")));

        final Schema commonSchema = bySubject.get("com.example.shared.Currency");
        assertThat(commonSchema.state(), is(Status.STATE.IGNORED));

        final Schema tradeSchema = bySubject.get("com.example.trading._public.trade-value");
        assertThat(tradeSchema.state(), is(Status.STATE.CREATED));
    }

    @Test
    @Order(3)
    void shouldNotProvisionAnythingIfNothingHasChanged() {
        // When:
        final List<Schema> provision =
                SchemaProvisioner.provision(
                        false,
                        false,
                        API_SPEC,
                        "./src/test/resources/schema-ref",
                        KAFKA_ENV.srClient());

        // Then:
        final Map<String, Schema> bySubject =
                provision.stream().collect(Collectors.toMap(Schema::subject, Function.identity()));
        assertThat(bySubject.keySet(), is(Set.of("com.example.shared.Currency")));

        final Schema commonSchema = bySubject.get("com.example.shared.Currency");
        assertThat(commonSchema.state(), is(Status.STATE.IGNORED));
    }

    @Test
    @Order(4)
    void shouldProvisionFromClassPath() {
        // When:
        final List<Schema> provisioned =
                SchemaProvisioner.provision(
                        false, false, API_SPEC, "schema-ref", KAFKA_ENV.srClient());

        // Then:
        assertThat(
                provisioned.stream().filter(topic -> topic.state() == Status.STATE.FAILED).count(),
                is(0L));
    }
}
