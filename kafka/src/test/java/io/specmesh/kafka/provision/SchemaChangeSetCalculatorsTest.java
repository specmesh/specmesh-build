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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.specmesh.kafka.DockerKafkaEnvironment;
import io.specmesh.kafka.KafkaEnvironment;
import io.specmesh.kafka.provision.SchemaProvisioner.Schema;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class SchemaChangeSetCalculatorsTest {

    @RegisterExtension
    private static final KafkaEnvironment KAFKA_ENV = DockerKafkaEnvironment.builder().build();

    public static final String SCHEMA_FILENAME = "simple.provision_demo._public.user_signed_up";

    @Test
    void shouldOutputMessagesOnBorkedSchema() throws Exception {

        final var client = srClient();

        client.register("subject", getSchema(".avsc", readFile(".avsc")));

        final var existing =
                List.of(
                        Schema.builder()
                                .type("AVRO")
                                .subject("subject")
                                .state(Status.STATE.READ)
                                .payload(readFile(".avsc"))
                                .build());
        final var required =
                List.of(
                        Schema.builder()
                                .subject("subject")
                                .type("AVRO")
                                .state(Status.STATE.READ)
                                .payload(readFile("-v3-bad.avsc"))
                                .build());

        final var calculator = SchemaChangeSetCalculators.builder().build(client);

        final var schemas = calculator.calculate(existing, required);
        assertThat(schemas.iterator().next().state(), is(Status.STATE.FAILED));
        assertThat(schemas.iterator().next().messages(), is(containsString("Incompatibility")));
        assertThat(
                schemas.iterator().next().messages(),
                is(containsString("type:READER_FIELD_MISSING_DEFAULT_VALUE")));
    }

    private static String readFile(final String extra) {
        try {
            return Files.readString(
                    Path.of("./build/resources/test/schema/" + SCHEMA_FILENAME + extra));
        } catch (Exception ex) {
            throw new RuntimeException(new File(".").getAbsolutePath(), ex);
        }
    }

    private static CachedSchemaRegistryClient srClient() {
        return new CachedSchemaRegistryClient(
                KAFKA_ENV.schemeRegistryServer(),
                5,
                List.of(
                        new ProtobufSchemaProvider(),
                        new AvroSchemaProvider(),
                        new JsonSchemaProvider()),
                Map.of());
    }

    static ParsedSchema getSchema(final String schemaRefType, final String content) {

        if (schemaRefType.endsWith(".avsc")) {
            return new AvroSchema(content);
        }
        if (schemaRefType.endsWith(".yml")) {
            return new JsonSchema(content);
        }
        if (schemaRefType.endsWith(".proto")) {
            return new ProtobufSchema(content);
        }
        throw new Provisioner.ProvisioningException("Unsupported schema type");
    }
}
