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
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.specmesh.kafka.provision.Status;
import io.specmesh.kafka.provision.schema.SchemaProvisioner.Schema;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SchemaChangeSetCalculatorsTest {

    private static final String DOMAIN_ID = "simple.provision_demo";
    private static final String SCHEMA_BASE = "simple.provision_demo._public.";

    private String subject;

    @BeforeEach
    void setUp() {
        subject = "subject." + UUID.randomUUID();
    }

    @Test
    void shouldDetectWhenSchemasHaveChanged() {
        // Given:
        final List<Schema> existing =
                List.of(
                        Schema.builder()
                                .type("AVRO")
                                .subject(subject)
                                .state(Status.STATE.READ)
                                .schema(loadSchema(SCHEMA_BASE + "user_signed_up.avsc").copy(1))
                                .build());

        final List<Schema> required =
                List.of(
                        Schema.builder()
                                .type("AVRO")
                                .subject(subject)
                                .state(Status.STATE.READ)
                                .schema(loadSchema(SCHEMA_BASE + "user_signed_up-v2.avsc"))
                                .build());

        final var calculator = SchemaChangeSetCalculators.builder().build(false);

        // When:
        final Collection<Schema> schemas = calculator.calculate(existing, required, DOMAIN_ID);

        // Then:
        assertThat(schemas, hasSize(1));
        final Schema schema = schemas.iterator().next();
        assertThat(schema.state(), is(Status.STATE.UPDATE));
        assertThat(schema.subject(), is(subject));
        assertThat(schema.messages(), is("\n Update"));
    }

    @Test
    void shouldDetectWhenSchemasHaveNotChanged() {
        // Given:
        final List<Schema> existing =
                List.of(
                        Schema.builder()
                                .type("AVRO")
                                .subject(subject)
                                .state(Status.STATE.READ)
                                .schema(loadSchema(SCHEMA_BASE + "user_signed_up.avsc").copy(1))
                                .build());

        final List<Schema> required =
                List.of(
                        Schema.builder()
                                .type("AVRO")
                                .subject(subject)
                                .state(Status.STATE.READ)
                                .schema(loadSchema(SCHEMA_BASE + "user_signed_up.avsc"))
                                .build());

        final var calculator = SchemaChangeSetCalculators.builder().build(false);

        // When:
        final Collection<Schema> schemas = calculator.calculate(existing, required, DOMAIN_ID);

        // Then:
        assertThat(schemas, is(empty()));
    }

    @Test
    void shouldIgnoreSchemasOutsideOfDomain() {
        // Given:
        final List<Schema> existing = List.of();

        final List<Schema> required =
                List.of(
                        Schema.builder()
                                .type("AVRO")
                                .subject(subject)
                                .state(Status.STATE.READ)
                                .schema(loadSchema("other.domain.Common.avsc"))
                                .build());

        final var calculator = SchemaChangeSetCalculators.builder().build(false);

        // When:
        final Collection<Schema> schemas = calculator.calculate(existing, required, DOMAIN_ID);

        // Then:
        assertThat(schemas, hasSize(1));
        final Schema schema = schemas.iterator().next();
        assertThat(schema.state(), is(Status.STATE.IGNORED));
        assertThat(schema.subject(), is(subject));
        assertThat(schema.messages(), is("\n ignored as it does not belong to the domain"));
    }

    private static ParsedSchema loadSchema(final String fileName) {
        return new SchemaReaders.FileSystemSchemaReader()
                .readLocal(Path.of("./src/test/resources/schema/" + fileName))
                .iterator()
                .next()
                .schema();
    }
}
