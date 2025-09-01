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
import org.hamcrest.Matcher;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;
import picocli.CommandLine;

/** Functional test of specs that use schema files that reference other schema files. */
@TestMethodOrder(OrderAnnotation.class)
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

    @BeforeAll
    static void beforeAll() {
        givenSchemaFromOtherDomainsAreRegistered();
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

    private static void testProvision(
            final String rootType, final boolean requireShared, final boolean expectShared) {
        final Provision provision = new Provision();
        new CommandLine(provision)
                .parseArgs(
                        "--bootstrap-server",
                        KAFKA_ENV.kafkaBootstrapServers(),
                        "--spec",
                        "examples/references/avro/" + rootType + ".spec.yaml",
                        "--username",
                        "admin",
                        "--secret",
                        "admin-secret",
                        "--schema-registry",
                        KAFKA_ENV.schemaRegistryServer(),
                        "--schema-path",
                        "./src/test/resources/examples/references/avro");

        // When:
        final var status = provision.run();

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
