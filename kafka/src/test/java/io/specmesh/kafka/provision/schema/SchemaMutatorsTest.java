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

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.specmesh.kafka.DockerKafkaEnvironment;
import io.specmesh.kafka.KafkaEnvironment;
import io.specmesh.kafka.provision.Status;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class SchemaMutatorsTest {

    @RegisterExtension
    private static final KafkaEnvironment KAFKA_ENV = DockerKafkaEnvironment.builder().build();

    private static final String SCHEMA_BASE = "simple.provision_demo._public.";

    private SchemaRegistryClient client;
    private String subject;

    @BeforeEach
    void setUp() {
        client = KAFKA_ENV.srClient();
        subject = "subject." + UUID.randomUUID();
    }

    @Test
    void shouldOutputMessagesOnBorkedSchema() throws Exception {
        // Given:
        client.register(subject, loadSchema(SCHEMA_BASE + "user_signed_up.avsc"));

        final List<SchemaProvisioner.Schema> required =
                List.of(
                        SchemaProvisioner.Schema.builder()
                                .type("AVRO")
                                .subject(subject)
                                .state(Status.STATE.UPDATE)
                                .schema(loadSchema(SCHEMA_BASE + "user_signed_up-v3-bad.avsc"))
                                .build());

        final var mutators = SchemaMutators.builder().schemaRegistryClient(client).build();

        // When:
        final Collection<SchemaProvisioner.Schema> schemas = mutators.mutate(required);

        // Then:
        assertThat(schemas.iterator().next().state(), is(Status.STATE.FAILED));
        assertThat(
                schemas.iterator().next().messages(),
                is(containsString("READER_FIELD_MISSING_DEFAULT_VALUE")));
        assertThat(schemas.iterator().next().messages(), is(containsString("borked")));
    }

    private static ParsedSchema loadSchema(final String fileName) {
        return new SchemaReaders.FileSystemSchemaReader()
                .readLocal(Path.of("./src/test/resources/schema/" + fileName))
                .iterator()
                .next()
                .schema();
    }
}
