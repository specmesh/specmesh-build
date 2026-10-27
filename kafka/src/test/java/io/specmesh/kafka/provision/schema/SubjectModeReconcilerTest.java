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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.specmesh.kafka.provision.Status.STATE;
import io.specmesh.kafka.provision.schema.SchemaProvisioner.Part;
import io.specmesh.kafka.provision.schema.SchemaProvisioner.Schema;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SubjectModeReconcilerTest {

    private static final String SUBJECT = "some.topic-key";

    private SchemaRegistryClient client;

    @BeforeEach
    void setUp() throws Exception {
        client = new MockSchemaRegistryClient();
        client.register(SUBJECT, new AvroSchema("\"string\""));
    }

    @Test
    void shouldMakeCreatedKeySubjectReadOnly() throws Exception {
        // Given:
        final Schema schema = keySchema().state(STATE.CREATED).build();

        // When:
        final List<Schema> result = reconcile(false, schema, List.of(schema));

        // Then:
        assertThat(client.getMode(SUBJECT), is("READONLY"));
        assertThat(result, contains(schema));
        assertThat(schema.state(), is(STATE.CREATED));
        assertThat(schema.messages(), containsString("Subject mode set to: READONLY"));
    }

    @Test
    void shouldMakeUnchangedKeySubjectReadOnly() throws Exception {
        // Given:
        final Schema schema = keySchema().build();

        // When:
        final List<Schema> result = reconcile(false, schema, List.of());

        // Then:
        assertThat(client.getMode(SUBJECT), is("READONLY"));
        assertThat(result, contains(schema));
        assertThat(schema.state(), is(STATE.UPDATED));
    }

    @Test
    void shouldMakeKeySubjectInImportModeReadOnly() throws Exception {
        // Given:
        client.setMode("IMPORT", SUBJECT, true);

        // When:
        reconcile(false, keySchema().build(), List.of());

        // Then:
        assertThat(client.getMode(SUBJECT), is("READONLY"));
    }

    @Test
    void shouldNotOutputKeySubjectThatIsAlreadyReadOnly() throws Exception {
        // Given:
        client.setMode("READONLY", SUBJECT);

        // Then:
        assertThat(reconcile(false, keySchema().build(), List.of()), is(empty()));
    }

    @Test
    void shouldLeaveKeySubjectInReadOnlyOverrideMode() throws Exception {
        // Given:
        client.setMode("READONLY_OVERRIDE", SUBJECT);

        // When:
        final List<Schema> result = reconcile(false, keySchema().build(), List.of());

        // Then:
        assertThat(result, is(empty()));
        assertThat(client.getMode(SUBJECT), is("READONLY_OVERRIDE"));
    }

    @Test
    void shouldNotMakeValueSubjectReadOnly() throws Exception {
        // When:
        final List<Schema> result =
                reconcile(false, keySchema().part(Optional.of(Part.VALUE)).build(), List.of());

        // Then:
        assertThat(result, is(empty()));
        assertThat(new SubjectModes(client).get(SUBJECT), is(Optional.empty()));
    }

    @Test
    void shouldNotMakeFailedSchemaReadOnly() throws Exception {
        // Given:
        final Schema schema = keySchema().state(STATE.FAILED).build();

        // When:
        reconcile(false, schema, List.of(schema));

        // Then:
        assertThat(new SubjectModes(client).get(SUBJECT), is(Optional.empty()));
    }

    @Test
    void shouldReportButNotChangeModeOfUnchangedKeySubjectOnDryRun() throws Exception {
        // Given:
        final Schema schema = keySchema().build();

        // When:
        final List<Schema> result = reconcile(true, schema, List.of());

        // Then:
        assertThat(result, contains(schema));
        assertThat(schema.state(), is(STATE.UPDATE));
        assertThat(schema.messages(), containsString("Subject mode would be set to: READONLY"));
        assertThat(new SubjectModes(client).get(SUBJECT), is(Optional.empty()));
    }

    @Test
    void shouldReportModeOfKeySubjectToBeCreatedOnDryRun() {
        // Given:
        final Schema schema = keySchema().subject("new-key").build();

        // When:
        reconcile(true, schema, List.of(schema));

        // Then:
        assertThat(schema.state(), is(STATE.CREATE));
        assertThat(schema.messages(), containsString("Subject mode would be set to: READONLY"));
    }

    @Test
    void shouldFailSchemaIfModeCannotBeSet() throws Exception {
        // Given:
        client = mock(SchemaRegistryClient.class);
        when(client.getMode(SUBJECT)).thenReturn("READWRITE");
        when(client.setMode(any(), any()))
                .thenThrow(new RestClientException("forbidden", 403, 40301));
        final Schema schema = keySchema().build();

        // When:
        final List<Schema> result = reconcile(false, schema, List.of());

        // Then:
        assertThat(result, contains(schema));
        assertThat(schema.state(), is(STATE.FAILED));
        assertThat(schema.exception().getMessage(), containsString("mode.mutability"));
    }

    private List<Schema> reconcile(
            final boolean dryRun, final Schema required, final List<Schema> changeset) {
        return new SubjectModeReconciler(dryRun, new SubjectModes(client))
                .reconcile(List.of(required), changeset);
    }

    private static Schema.SchemaBuilder keySchema() {
        return Schema.builder()
                .subject(SUBJECT)
                .state(STATE.CREATE)
                .topicSchema(true)
                .part(Optional.of(Part.KEY))
                .schemaIdLocation(Optional.of("payload"));
    }
}
