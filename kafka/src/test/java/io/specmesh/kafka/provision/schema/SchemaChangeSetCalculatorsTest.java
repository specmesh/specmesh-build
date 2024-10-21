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
package io.specmesh.kafka.provision.schema;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import io.specmesh.kafka.DockerKafkaEnvironment;
import io.specmesh.kafka.KafkaEnvironment;
import io.specmesh.kafka.provision.Status;
import io.specmesh.kafka.provision.schema.SchemaProvisioner.Schema;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class SchemaChangeSetCalculatorsTest {

    @RegisterExtension
    private static final KafkaEnvironment KAFKA_ENV = DockerKafkaEnvironment.builder().build();

    public static final String SCHEMA_FILENAME = "simple.provision_demo._public.user_signed_up";

    @Test
    void shouldOutputMessagesOnBorkedSchema() throws Exception {

        final var client = KAFKA_ENV.srClient();

        client.register(
                "subject",
                new SchemaReaders.FileSystemSchemaReader()
                        .readLocal(filename(".avsc"))
                        .iterator()
                        .next());

        final var existing =
                List.of(
                        Schema.builder()
                                .type("AVRO")
                                .subject("subject")
                                .state(Status.STATE.READ)
                                .schemas(
                                        new SchemaReaders.FileSystemSchemaReader()
                                                .readLocal(filename(".avsc")))
                                .build());
        final var required =
                List.of(
                        Schema.builder()
                                .subject("subject")
                                .type("AVRO")
                                .state(Status.STATE.READ)
                                .schemas(
                                        new SchemaReaders.FileSystemSchemaReader()
                                                .readLocal(filename("-v3-bad.avsc")))
                                .build());

        final var calculator = SchemaChangeSetCalculators.builder().build(false, client);

        final var schemas = calculator.calculate(existing, required);
        assertThat(schemas.iterator().next().state(), is(Status.STATE.FAILED));
        assertThat(
                schemas.iterator().next().messages(),
                is(containsString("READER_FIELD_MISSING_DEFAULT_VALUE")));
        assertThat(schemas.iterator().next().messages(), is(containsString("borked")));
    }

    private static Path filename(final String extra) {
        return Path.of("./src/test/resources/schema/" + SCHEMA_FILENAME + extra);
    }
}
