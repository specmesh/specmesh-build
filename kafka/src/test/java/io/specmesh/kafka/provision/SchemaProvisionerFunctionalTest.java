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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.specmesh.kafka.DockerKafkaEnvironment;
import io.specmesh.kafka.KafkaApiSpec;
import io.specmesh.kafka.KafkaEnvironment;
import io.specmesh.kafka.provision.Status.STATE;
import io.specmesh.kafka.provision.schema.SchemaProvisioner;
import io.specmesh.kafka.provision.schema.SchemaProvisioner.Schema;
import io.specmesh.test.TestSpecLoader;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Tests execution DryRuns and UPDATES where the provisioner-functional-test-api.yml is already
 * provisioned
 */
@SuppressFBWarnings(
        value = "IC_INIT_CIRCULARITY",
        justification = "shouldHaveInitializedEnumsCorrectly() proves this is false positive")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SchemaProvisionerFunctionalTest {

    private static final KafkaApiSpec API_SPEC =
            TestSpecLoader.loadFromClassPath("provisioner-functional-test-api.yaml");

    private static final KafkaApiSpec API_UPDATE_SPEC =
            TestSpecLoader.loadFromClassPath("provisioner-update-functional-test-api.yaml");
    private SchemaRegistryClient srClient;

    private enum Domain {
        /** The domain associated with the spec. */
        SELF(API_SPEC.id()),
        /** An unrelated domain. */
        UNRELATED("london.hammersmith.transport"),
        /** A domain granted access to the protected topic. */
        LIMITED("some.other.domain.root");

        final String domainId;

        Domain(final String name) {
            this.domainId = name;
        }
    }

    private static final String ADMIN_USER = "admin";

    @RegisterExtension
    private static final KafkaEnvironment KAFKA_ENV =
            DockerKafkaEnvironment.builder()
                    .withSaslAuthentication(
                            ADMIN_USER,
                            ADMIN_USER + "-secret",
                            Domain.SELF.domainId,
                            Domain.SELF.domainId + "-secret")
                    .build();

    @BeforeEach
    void setUp() {
        srClient = KAFKA_ENV.srClient();
    }

    @AfterEach
    void tearDown() throws Exception {
        srClient.close();
    }

    /** setup for following update tests */
    @Test
    @Order(1)
    void shouldProvisionExistingSpec() throws Exception {
        // When:
        final Collection<Schema> provisioned =
                SchemaProvisioner.provision(
                        false, false, API_SPEC, "./build/resources/test", srClient);

        // Then: should have provisioned 3 schema:
        assertThat(
                provisioned.stream().filter(topic -> topic.state() == STATE.CREATED).count(),
                is(3L));

        assertThat(
                srClient.getAllSubjects(),
                containsInAnyOrder(
                        "simple.provision_demo._public.user_signed_up-key",
                        "simple.provision_demo._public.user_signed_up-value",
                        "simple.provision_demo._protected.user_info-value"));

        assertThat(
                srClient.getAllSubjectsByPrefix(API_SPEC.id()),
                hasSize(srClient.getAllSubjects().size()));
    }

    @Test
    @Order(2)
    void shouldPublishUpdatedSchemas() throws Exception {

        final Collection<Schema> dryRunChangeset =
                SchemaProvisioner.provision(
                        true, false, API_UPDATE_SPEC, "./build/resources/test", srClient);

        // Verify - the Update is proposed
        assertThat(
                dryRunChangeset.stream().filter(topic -> topic.state() == STATE.UPDATE).count(),
                is(1L));

        // Verify - should have 3 SR entries (1 was updated, 2 was from original spec)
        assertThat(
                srClient.getAllSubjects(),
                containsInAnyOrder(
                        "simple.provision_demo._public.user_signed_up-key",
                        "simple.provision_demo._public.user_signed_up-value",
                        "simple.provision_demo._protected.user_info-value"));

        final Collection<Schema> updateChangeset =
                SchemaProvisioner.provision(
                        false, false, API_UPDATE_SPEC, "./build/resources/test", srClient);

        final var parsedSchemas =
                srClient.getSchemas(updateChangeset.iterator().next().subject(), false, true);
        // should now contain the address field
        assertThat(parsedSchemas.get(0).canonicalString(), is(containsString("address")));

        // Verify - 1 Update has been executed
        assertThat(updateChangeset, is(hasSize(1)));

        final var schemas = srClient.getSchemas("simple", false, false);

        final var schemaNames =
                schemas.stream().map(ParsedSchema::name).collect(Collectors.toSet());

        assertThat(
                schemaNames,
                is(
                        containsInAnyOrder(
                                "io.specmesh.kafka.schema.UserInfo",
                                "simple.provision_demo._public.user_signed_up_value.key.UserSignedUpKey",
                                "simple.provision_demo._public.user_signed_up_value.UserSignedUp")));
    }

    @Test
    @Order(3)
    void shouldRemoveUnspecdSchemas() throws RestClientException, IOException {

        final var subject = "simple.provision_demo._public.NOT_user_signed_up-value";
        final var schemaContent =
                "{\n"
                    + "  \"type\": \"record\",\n"
                    + "  \"namespace\": \"simple.provision_demo._public.user_signed_up_value\",\n"
                    + "  \"name\": \"UserSignedUp\",\n"
                    + "  \"fields\": [\n"
                    + "    {\"name\": \"fullName\", \"type\": \"string\"},\n"
                    + "    {\"name\": \"age\", \"type\": \"int\"}\n"
                    + "  ]\n"
                    + "}";
        final var schema =
                Schema.builder()
                        .subject(subject)
                        .type("/schema/simple.provision_demo._public.user_signed_up.avsc")
                        .schemas(List.of(new AvroSchema(schemaContent)))
                        .state(STATE.READ)
                        .build();

        // insert the bad schema
        srClient.register(subject, schema.getSchema());

        testDryRun(subject);
        testCleanUnSpecSchemas();
    }

    private void testCleanUnSpecSchemas() throws IOException, RestClientException {
        final var cleanerSet2 =
                SchemaProvisioner.provision(
                        false, true, API_UPDATE_SPEC, "./build/resources/test", srClient);

        // verify it was removed
        assertThat(cleanerSet2.iterator().next().state(), is(STATE.DELETED));

        // verify removal
        assertThat(
                srClient.getAllSubjectsByPrefix(API_SPEC.id()),
                containsInAnyOrder(
                        "simple.provision_demo._public.user_signed_up-key",
                        "simple.provision_demo._public.user_signed_up-value",
                        "simple.provision_demo._protected.user_info-value"));
    }

    private void testDryRun(final String subject) throws IOException, RestClientException {
        // test dry run
        final var cleanerSet =
                SchemaProvisioner.provision(
                        true, true, API_UPDATE_SPEC, "./build/resources/test", srClient);

        // verify dry run
        assertThat(cleanerSet, is(hasSize(1)));
        assertThat(
                cleanerSet.stream().map(Schema::subject).collect(Collectors.toList()),
                contains(subject));
        // verify intent to DELETE
        assertThat(cleanerSet.iterator().next().state(), is(STATE.DELETE));

        final var allSchemasforId = srClient.getAllSubjectsByPrefix(API_SPEC.id());
        assertThat(allSchemasforId, is(hasSize(4)));
    }
}
