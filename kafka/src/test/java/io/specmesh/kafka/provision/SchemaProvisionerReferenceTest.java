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
import java.util.ArrayList;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Provision schemas with references - missing refs will fail. For common schema set the
 * schemaLookupStrategy: "RecordNameStrategy" so Trade can reference the subject like that
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SchemaProvisionerReferenceTest {

    private static final KafkaApiSpec COMMON_API_SPEC =
            TestSpecLoader.loadFromClassPath("schema-ref/com.example.shared-api.yml");

    private static final KafkaApiSpec API_SPEC =
            TestSpecLoader.loadFromClassPath("schema-ref/com.example.trading-api.yml");

    private static final KafkaApiSpec SPEC_WITH_REFS_API_SPEC =
            TestSpecLoader.loadFromClassPath(
                    "schema-ref/com.example.single-spec-with-refs-api.yml");

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
    void shouldProvisionSpecWithMissingRefToCurrency() {

        final var provision =
                SchemaProvisioner.provision(
                        false,
                        false,
                        API_SPEC,
                        "./src/test/resources/schema-ref",
                        KAFKA_ENV.srClient());

        final var schemaState = provision.iterator().next();
        assertThat(
                "Failed to load with:" + schemaState.toString(),
                schemaState.state(),
                Matchers.is(Status.STATE.FAILED));
    }
    /** publish common schema (via api) and also domain-owned schema */
    @Test
    void shouldProvisionTwoSpecsWithRefs() {

        final var provisionCommon =
                SchemaProvisioner.provision(
                        false,
                        false,
                        COMMON_API_SPEC,
                        "./src/test/resources/schema-ref",
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
                        "./src/test/resources/schema-ref",
                        KAFKA_ENV.srClient());

        final var schemaState = provision.iterator().next();
        assertThat(
                "Failed to load with:" + schemaState.toString(),
                schemaState.state(),
                Matchers.is(Status.STATE.CREATED));
    }

    @Test
    void shouldProvisionSpecsWithMultipleRefsSoThatZeroRefsRegisterFirst() {

        final var provisionBothSchemas =
                SchemaProvisioner.provision(
                        false,
                        false,
                        SPEC_WITH_REFS_API_SPEC,
                        "./src/test/resources/schema-ref",
                        KAFKA_ENV.srClient());

        final var schemas = new ArrayList<>(provisionBothSchemas);

        assertThat(schemas, Matchers.hasSize(2));

        final var currencySchemaState = schemas.get(0);
        assertThat(
                "Failed to load with:" + currencySchemaState.subject(),
                currencySchemaState.subject(),
                Matchers.is("com.example.shared.Currency"));

        assertThat(
                "Failed to load with:" + currencySchemaState,
                currencySchemaState.state(),
                Matchers.is(Status.STATE.CREATED));

        final var tradeSchemaState = schemas.get(1);
        assertThat(
                "Failed to load with:" + tradeSchemaState.subject(),
                tradeSchemaState.subject(),
                Matchers.is("com.example.refs._public.trade-value"));

        assertThat(
                "Failed to load with:" + tradeSchemaState,
                tradeSchemaState.state(),
                Matchers.is(Status.STATE.CREATED));
    }
}
