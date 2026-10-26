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

import io.specmesh.kafka.provision.Status.STATE;
import io.specmesh.kafka.provision.schema.SchemaProvisioner.Part;
import io.specmesh.kafka.provision.schema.SchemaProvisioner.Schema;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class SchemaChangePolicyTest {

    private static final String SUBJECT = "some.topic-key";

    private final SchemaChangePolicy policy = new SchemaChangePolicy();

    @Test
    void shouldFailKeyChangeWhenSchemaIdIsInPayload() {
        // When:
        final Schema result = policy.enforce(failingChange().build());

        // Then:
        assertThat(result.state(), is(STATE.FAILED));
        assertThat(result.exception().getMessage(), containsString(SUBJECT));
        assertThat(result.exception().getMessage(), containsString("schemaIdLocation"));
    }

    @Test
    void shouldFailKeyChangeWhenSchemaIdLocationNotSet() {
        assertThat(
                policy.enforce(failingChange().schemaIdLocation(Optional.empty()).build()).state(),
                is(STATE.FAILED));
    }

    @Test
    void shouldAllowKeyChangeWhenSchemaIdIsInHeader() {
        assertThat(
                policy.enforce(failingChange().schemaIdLocation(Optional.of("header")).build())
                        .state(),
                is(STATE.UPDATE));
    }

    @Test
    void shouldIgnoreCaseOfSchemaIdLocation() {
        assertThat(
                policy.enforce(failingChange().schemaIdLocation(Optional.of("HEADER")).build())
                        .state(),
                is(STATE.UPDATE));
    }

    @Test
    void shouldAllowValueChangeWhenSchemaIdIsInPayload() {
        assertThat(
                policy.enforce(failingChange().part(Optional.of(Part.VALUE)).build()).state(),
                is(STATE.UPDATE));
    }

    @Test
    void shouldAllowChangeToSchemaThatIsNotTopicSchema() {
        assertThat(
                policy.enforce(failingChange().topicSchema(false).build()).state(),
                is(STATE.UPDATE));
    }

    @Test
    void shouldAllowCreatingKeySchema() {
        assertThat(
                policy.enforce(failingChange().state(STATE.CREATE).build()).state(),
                is(STATE.CREATE));
    }

    /** A change that fails. Tests override the one thing that makes them different. */
    private static Schema.SchemaBuilder failingChange() {
        return Schema.builder()
                .subject(SUBJECT)
                .state(STATE.UPDATE)
                .topicSchema(true)
                .part(Optional.of(Part.KEY))
                .schemaIdLocation(Optional.of("payload"));
    }
}
