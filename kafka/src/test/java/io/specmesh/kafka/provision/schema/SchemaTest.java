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
import static org.hamcrest.Matchers.is;

import io.specmesh.kafka.provision.schema.SchemaProvisioner.Part;
import io.specmesh.kafka.provision.schema.SchemaProvisioner.Schema;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class SchemaTest {

    @Test
    void shouldBeKeyWithSchemaIdInPayload() {
        assertThat(keyWithSchemaIdInPayload().build().isKeyWithSchemaIdInPayload(), is(true));
    }

    @Test
    void shouldBeKeyWithSchemaIdInPayloadWhenSchemaIdLocationNotSet() {
        assertThat(
                keyWithSchemaIdInPayload()
                        .schemaIdLocation(Optional.empty())
                        .build()
                        .isKeyWithSchemaIdInPayload(),
                is(true));
    }

    @Test
    void shouldNotBeKeyWithSchemaIdInPayloadWhenSchemaIdIsInHeader() {
        assertThat(
                keyWithSchemaIdInPayload()
                        .schemaIdLocation(Optional.of("header"))
                        .build()
                        .isKeyWithSchemaIdInPayload(),
                is(false));
    }

    @Test
    void shouldIgnoreCaseOfSchemaIdLocation() {
        assertThat(
                keyWithSchemaIdInPayload()
                        .schemaIdLocation(Optional.of("HEADER"))
                        .build()
                        .isKeyWithSchemaIdInPayload(),
                is(false));
    }

    @Test
    void shouldNotBeKeyWithSchemaIdInPayloadWhenValue() {
        assertThat(
                keyWithSchemaIdInPayload()
                        .part(Optional.of(Part.VALUE))
                        .build()
                        .isKeyWithSchemaIdInPayload(),
                is(false));
    }

    @Test
    void shouldNotBeKeyWithSchemaIdInPayloadWhenNotTopicSchema() {
        assertThat(
                keyWithSchemaIdInPayload().topicSchema(false).build().isKeyWithSchemaIdInPayload(),
                is(false));
    }

    private static Schema.SchemaBuilder keyWithSchemaIdInPayload() {
        return Schema.builder()
                .subject("some.topic-key")
                .topicSchema(true)
                .part(Optional.of(Part.KEY))
                .schemaIdLocation(Optional.of("payload"));
    }
}
