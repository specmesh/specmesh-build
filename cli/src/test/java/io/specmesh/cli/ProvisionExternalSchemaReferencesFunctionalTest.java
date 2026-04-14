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

package io.specmesh.cli;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.MethodOrderer.OrderAnnotation;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.specmesh.kafka.DockerKafkaEnvironment;
import io.specmesh.kafka.KafkaEnvironment;
import io.specmesh.kafka.provision.Status;
import io.specmesh.kafka.provision.TopicProvisioner.Topic;
import io.specmesh.kafka.provision.schema.SchemaProvisioner;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

/** Functional test of specs that use schema files that reference other schema files. */
@TestMethodOrder(OrderAnnotation.class)
@Tag("ContainerisedTest")
class ProvisionExternalSchemaReferencesFunctionalTest {

    private static final String DOMAIN_USER = "schema.reference.demo";
    private static final Path SCHEMA_ROOT =
            Path.of("./src/test/resources/examples/references/avro/schema");

    @RegisterExtension
    private static final KafkaEnvironment KAFKA_ENV =
            DockerKafkaEnvironment.builder()
                    .withSaslAuthentication(
                            "admin", "admin-secret", DOMAIN_USER, DOMAIN_USER + "-secret")
                    .withKafkaAcls()
                    .build();

    @TempDir Path tempDir;

    private String domainId;

    @BeforeAll
    static void beforeAll() {
        givenSchemaFromOtherDomainsAreRegistered();
    }

    @BeforeEach
    void setUp() {
        domainId = "some.domain." + UUID.randomUUID().toString().toLowerCase();
    }

    @Order(1)
    @Test
    void shouldProvisionTopLevelRecord() {
        testProvision("record", true, true);
    }

    @Test
    void shouldProvisionTopLevelUnion() {
        testProvision("union", false, true);
    }

    @Test
    void shouldProvisionTopLevelMap() {
        testProvision("map", false, true);
    }

    @Test
    void shouldProvisionTopLevelArray() {
        testProvision("array", false, true);
    }

    @Test
    void shouldProvisionTopLevelPrimitive() {
        testProvision("primitive", false, false);
    }

    @Test
    void shouldProvisionAddedNestedSchema() {
        // Given:
        final String initialSchemaC =
                """
                {
                  "type": "record",
                  "name": "C",
                  "namespace": "{domain.id}",
                  "fields": [
                    {"name": "initial", "type": "string"}
                  ]
                }
                """;

        assertThat(provision(Map.of("C", initialSchemaC)).failed(), is(false));

        final String schemaA =
                """
                {
                  "type": "record",
                  "name": "A",
                  "namespace": "{domain.id}",
                  "fields": [
                    {"name": "val", "type": "string"}
                  ]
                }
                """;

        final String schemaB =
                """
                {
                  "type": "record",
                  "name": "B",
                  "namespace": "{domain.id}",
                  "fields": [
                    {"name": "val", "type": "A"}
                  ]
                }
                """;

        final String updatedSchemaC =
                """
                {
                  "type": "record",
                  "name": "C",
                  "namespace": "{domain.id}",
                  "fields": [
                    {"name": "initial", "type": "string"},
                    {"name": "added", "type": ["null", "B"], "default": null}
                  ]
                }
                """;

        // When:
        final Status status =
                provision(
                        Map.of(
                                "A", schemaA,
                                "B", schemaB,
                                "C", updatedSchemaC));

        // Then:
        status.check();
    }

    private Status provision(final Map<String, String> schema) {
        writeSchema(schema);
        final Path spec = writeSpec();
        final Provision provision = buildCommand(spec, tempDir);
        return provision.run();
    }

    private void writeSchema(final Map<String, String> schema) {
        for (final Map.Entry<String, String> e : schema.entrySet()) {
            final String entityName = e.getKey();
            final String content = e.getValue();
            final Path path = tempDir.resolve(domainId + "." + entityName + ".avsc");
            writeFile(path, content);
        }
    }

    private Path writeSpec() {
        final String content =
                """
                asyncapi: '2.4.0'
                id: 'urn:{domain.id}'
                info:
                  title: Avro Schema Reference Demo
                  version: '1.0.0'
                channels:
                  _public.thing:
                    bindings:
                      kafka:
                        partitions: 3

                    publish:
                      operationId: publishRecord
                      message:
                        bindings:
                          kafka:
                            key:
                              type: long
                        payload:
                          $ref: {domain.id}.C.avsc
                """;

        final Path path = tempDir.resolve(domainId + ".spec.avsc");
        return writeFile(path, content);
    }

    private Path writeFile(final Path path, final String content) {
        try {
            return Files.writeString(path, content.replace("{domain.id}", domainId));
        } catch (IOException ex) {
            throw new AssertionError("Failed to write to " + path, ex);
        }
    }

    private static void testProvision(
            final String rootType, final boolean requireShared, final boolean expectShared) {
        final Provision provision =
                buildCommand(
                        Path.of("examples/references/avro/" + rootType + ".spec.yaml"),
                        Path.of("./src/test/resources/examples/references/avro"));

        // When:
        final Status status = provision.run();

        // Then:
        assertThat(status.failed(), is(false));

        assertThat(
                status.topics().stream().map(Topic::name).toList(),
                contains("schema.reference.demo._public." + rootType));

        assertThat(
                status.schemas().stream()
                        .filter(s -> s.state() == Status.STATE.CREATED)
                        .map(SchemaProvisioner.Schema::subject)
                        .toList(),
                expectedCreatedSchemaSubjects(
                        "schema.reference.demo._public." + rootType + "-value", requireShared));

        if (expectShared) {
            assertThat(
                    status.schemas().stream()
                            .filter(s -> s.state() == Status.STATE.IGNORED)
                            .map(SchemaProvisioner.Schema::subject)
                            .toList(),
                    containsInAnyOrder(
                            "shared.SharedThing", "schema.reference.demo.sub.domain.SubThing"));
        }

        // When:
        final var statusRepublish = provision.run();

        // Then:
        assertThat(statusRepublish.failed(), is(false));
        assertThat(statusRepublish.topics(), is(empty()));
        assertThat(statusRepublish.acls(), is(empty()));

        final List<?> schemas =
                statusRepublish.schemas().stream()
                        .filter(s -> s.state() != Status.STATE.IGNORED)
                        .toList();

        assertThat(schemas, is(empty()));
    }

    private static Provision buildCommand(final Path spec, final Path schemaPath) {
        final Provision provision = new Provision();
        new CommandLine(provision)
                .parseArgs(
                        "--bootstrap-server",
                        KAFKA_ENV.kafkaBootstrapServers(),
                        "--spec",
                        spec.toString(),
                        "--username",
                        "admin",
                        "--secret",
                        "admin-secret",
                        "--schema-registry",
                        KAFKA_ENV.schemaRegistryServer(),
                        "--schema-path",
                        schemaPath.toString());
        return provision;
    }

    private static Matcher<Iterable<? extends String>> expectedCreatedSchemaSubjects(
            final String entitySubject, final boolean requireShared) {
        final Matcher<Iterable<? extends String>> all =
                containsInAnyOrder(
                        entitySubject,
                        "schema.reference.demo.ThingA",
                        "schema.reference.demo.ThingB",
                        "schema.reference.demo.ThingC",
                        "schema.reference.demo.ThingD",
                        "schema.reference.demo.ThingE");

        if (requireShared) {
            return all;
        }

        return either(contains(entitySubject)).or(all);
    }

    private static void givenSchemaFromOtherDomainsAreRegistered() {
        // Common schema registration covered by
        // https://github.com/specmesh/specmesh-build/issues/453.
        // Until then, handle manually:

        try (SchemaRegistryClient srClient = KAFKA_ENV.srClient()) {
            registerSchema("schema.reference.demo.sub.domain.SubThing", srClient);
            registerSchema("shared.SharedThing", srClient);
        } catch (Exception e) {
            throw new AssertionError("failed to close client", e);
        }
    }

    public static void registerSchema(final String name, final SchemaRegistryClient srClient) {
        final AvroSchema schema = new AvroSchema(readLocalSchema(name));
        try {
            final int id = srClient.register(name, schema);
            System.out.println("Registered " + name + " with id " + id);
        } catch (Exception e) {
            throw new AssertionError("failed to register common schema", e);
        }
    }

    private static String readLocalSchema(final String subject) {
        final Path path = SCHEMA_ROOT.resolve(subject + ".avsc").normalize();
        try {
            return Files.readString(path);
        } catch (IOException e) {
            throw new AssertionError("Failed to read schema: " + path.toAbsolutePath(), e);
        }
    }
}
