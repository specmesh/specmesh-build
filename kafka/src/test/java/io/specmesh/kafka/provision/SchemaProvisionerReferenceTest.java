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

import io.specmesh.kafka.DockerKafkaEnvironment;
import io.specmesh.kafka.KafkaApiSpec;
import io.specmesh.kafka.KafkaEnvironment;
import io.specmesh.kafka.provision.schema.SchemaProvisioner;
import io.specmesh.test.TestSpecLoader;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Provision schemas with references - missing refs will fail.
 * For common schema set the schemaLookupStrategy: "RecordNameStrategy"
 * so Trade can reference the subject like that
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
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
    void shouldProvisionSpecWithMissingRefToCurrency() {

        final var provision =
                SchemaProvisioner.provision(
                        false,
                        false,
                        API_SPEC,
                        "./build/resources/test/schema-ref",
                        KAFKA_ENV.srClient());

        final var schemaState = provision.iterator().next();
        assertThat(
                "Failed to load with:" + schemaState.toString(),
                schemaState.state(),
                Matchers.is(Status.STATE.FAILED));
    }
    /** publish common schema (via api) and also domain-owned schema */
    @Test
    @Order(2)
    void shouldProvisionSpecWithRefs() {

        final var provisionCommon =
                SchemaProvisioner.provision(
                        false,
                        false,
                        COMMON_API_SPEC,
                        "./build/resources/test/schema-ref",
                        KAFKA_ENV.srClient());

        final var commonSchemaState = provisionCommon.iterator().next();
        assertThat(
                "Failed to load with:" + commonSchemaState.toString(),
                commonSchemaState.state(),
                Matchers.is(Status.STATE.CREATED));

        final var provision =
                SchemaProvisioner.provision(
                        false,
                        false,
                        API_SPEC,
                        "./build/resources/test/schema-ref",
                        KAFKA_ENV.srClient());

        final var schemaState = provision.iterator().next();
        assertThat(
                "Failed to load with:" + schemaState.toString(),
                schemaState.state(),
                Matchers.is(Status.STATE.CREATED));
    }
}
