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

import static io.specmesh.cli.util.CommonSchema.OTHER_SCHEMA_SUBJECT;
import static io.specmesh.cli.util.CommonSchema.TOPIC_KEY_SCHEMA_SUBJECT;
import static io.specmesh.cli.util.CommonSchema.TOPIC_VALUE_SCHEMA_SUBJECT;
import static io.specmesh.cli.util.CommonSchema.registerCommonSchema;
import static io.specmesh.kafka.provision.Status.STATE.CREATED;
import static io.specmesh.kafka.provision.Status.STATE.FAILED;
import static io.specmesh.kafka.provision.Status.STATE.IGNORED;
import static java.util.stream.Collectors.toMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.specmesh.cli.util.CommonSchema;
import io.specmesh.kafka.DockerKafkaEnvironment;
import io.specmesh.kafka.KafkaApiSpec;
import io.specmesh.kafka.KafkaEnvironment;
import io.specmesh.kafka.provision.Provisioner;
import io.specmesh.kafka.provision.Status;
import io.specmesh.kafka.provision.schema.SchemaProvisioner;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Functional test for using common schema, i.e. shares schema that are registered by another domain
 */
@Tag("ContainerisedTest")
class ProvisionCommonSchemaFunctionalTest {

    private static final String OWNER_USER = "simple.schema_demo";

    private static final KafkaApiSpec SPEC =
            KafkaApiSpec.loadFromFileSystem("src/test/resources/shared_schema_demo-api.yaml");

    @RegisterExtension
    private static final KafkaEnvironment KAFKA_ENV =
            DockerKafkaEnvironment.builder()
                    .withSaslAuthentication(
                            "admin", "admin-secret", OWNER_USER, OWNER_USER + "-secret")
                    .withKafkaAcls()
                    .build();

    @Test
    void shouldFailIfSharedTopicSchemaNotRegistered() throws Exception {
        try (Admin admin = KAFKA_ENV.adminClient();
                SchemaRegistryClient srClient = KAFKA_ENV.srClient()) {

            // Given:
            CommonSchema.unregisterCommonSchema(srClient);

            final Provisioner provisioner =
                    Provisioner.builder()
                            .apiSpec(SPEC)
                            .adminClient(admin)
                            .schemaRegistryClient(srClient)
                            .schemaPath("src/test/resources/")
                            .build();

            // When:
            final Status status = provisioner.provision();

            // Then:
            assertThat(status.failed(), is(true));

            final Map<String, SchemaProvisioner.Schema> schemaBySubject =
                    status.schemas().stream()
                            .collect(toMap(SchemaProvisioner.Schema::subject, Function.identity()));

            assertThat(
                    schemaBySubject.keySet(),
                    is(
                            Set.of(
                                    OTHER_SCHEMA_SUBJECT,
                                    TOPIC_KEY_SCHEMA_SUBJECT,
                                    TOPIC_VALUE_SCHEMA_SUBJECT)));

            assertThat(schemaBySubject.get(OTHER_SCHEMA_SUBJECT).state(), is(IGNORED));
            assertThat(schemaBySubject.get(TOPIC_KEY_SCHEMA_SUBJECT).state(), is(FAILED));
            assertThat(schemaBySubject.get(TOPIC_VALUE_SCHEMA_SUBJECT).state(), is(FAILED));

            assertThat(
                    schemaBySubject.get(TOPIC_KEY_SCHEMA_SUBJECT).exception().getMessage(),
                    containsString(
                            "Topic schema that are not owned by the domain must already be"
                                + " registered under subject matching fully qualified name. name:"
                                + " other.domain.Common"));
        }
    }

    @Test
    void shouldSucceedIfSharedTopicSchemaAreRegisteredWithSubjectMatchingName() throws Exception {
        try (Admin admin = KAFKA_ENV.adminClient();
                SchemaRegistryClient srClient = KAFKA_ENV.srClient()) {

            // Given:
            registerCommonSchema(srClient);

            final Provisioner provisioner =
                    Provisioner.builder()
                            .apiSpec(SPEC)
                            .adminClient(admin)
                            .schemaRegistryClient(srClient)
                            .schemaPath("src/test/resources/")
                            .build();

            // When:
            final Status status = provisioner.provision();

            // Then:
            status.check();

            final Map<String, SchemaProvisioner.Schema> schemaBySubject =
                    status.schemas().stream()
                            .collect(toMap(SchemaProvisioner.Schema::subject, Function.identity()));

            assertThat(
                    schemaBySubject.keySet(),
                    is(
                            Set.of(
                                    OTHER_SCHEMA_SUBJECT,
                                    TOPIC_KEY_SCHEMA_SUBJECT,
                                    TOPIC_VALUE_SCHEMA_SUBJECT)));

            assertThat(schemaBySubject.get(OTHER_SCHEMA_SUBJECT).state(), is(IGNORED));
            assertThat(schemaBySubject.get(TOPIC_KEY_SCHEMA_SUBJECT).state(), is(CREATED));
            assertThat(schemaBySubject.get(TOPIC_VALUE_SCHEMA_SUBJECT).state(), is(CREATED));

            // When:
            final Status statusRepublish = provisioner.provision();

            // Then:
            status.check();

            final Map<String, SchemaProvisioner.Schema> republishSchemaBySubject =
                    statusRepublish.schemas().stream()
                            .collect(toMap(SchemaProvisioner.Schema::subject, Function.identity()));

            assertThat(republishSchemaBySubject.keySet(), is(Set.of(OTHER_SCHEMA_SUBJECT)));

            assertThat(republishSchemaBySubject.get(OTHER_SCHEMA_SUBJECT).state(), is(IGNORED));
        }
    }
}
