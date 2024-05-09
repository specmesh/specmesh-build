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

package io.specmesh.apiparser.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.databind.json.JsonMapper;
import io.specmesh.apiparser.AsyncApiParser.APIParserException;
import io.specmesh.apiparser.model.RecordPart.KafkaPart;
import io.specmesh.apiparser.model.RecordPart.KafkaPart.KafkaType;
import io.specmesh.apiparser.model.RecordPart.OtherPart;
import io.specmesh.apiparser.model.RecordPart.SchemaRefPart;
import io.specmesh.apiparser.parse.SpecMapper;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class MessageTest {

    private static final JsonMapper MAPPER = SpecMapper.mapper();

    @Test
    void shouldHandleMissingKey() throws Exception {
        // Given:
        final String json = "message.bindings:\n" + "  kafka:\n" + "payload:\n" + "  type: string";

        final Message message = MAPPER.readValue(json, Message.class);

        // When:
        final SchemaInfo schemas = message.schemas();

        // Then:
        assertThat(schemas.key(), is(Optional.empty()));
    }

    @Test
    void shouldHandleMissingKafkaBindings() throws Exception {
        // Given:
        final String json = "bindings:\n" + "payload:\n" + "  type: string";

        final Message message = MAPPER.readValue(json, Message.class);

        // When:
        final SchemaInfo schemas = message.schemas();

        // Then:
        assertThat(schemas.key(), is(Optional.empty()));
    }

    @Test
    void shouldHandleMissingBindings() throws Exception {
        // Given:
        final String json = "payload:\n" + "  type: string";

        final Message message = MAPPER.readValue(json, Message.class);

        // When:
        final SchemaInfo schemas = message.schemas();

        // Then:
        assertThat(schemas.key(), is(Optional.empty()));
    }

    @Test
    void shouldThrowOnMissingPayload() throws Exception {
        // Given:
        final String json = "bindings:\n" + "  kafka:\n" + "    key:\n" + "      type: long";

        final Message message = MAPPER.readValue(json, Message.class);

        // When:
        final Exception e = assertThrows(APIParserException.class, message::schemas);

        // Then:
        assertThat(e.getMessage(), startsWith("missing 'message.payload' definition"));
    }

    @Test
    void shouldExtractInbuiltKafkaSchemaInfo() throws Exception {
        // Given:
        final String json =
                "bindings:\n"
                        + "  kafka:\n"
                        + "    key:\n"
                        + "      type: long\n"
                        + "payload:\n"
                        + "  type: string";

        final Message message = MAPPER.readValue(json, Message.class);

        // When:
        final SchemaInfo schemas = message.schemas();

        // Then:
        assertThat(schemas.key(), is(Optional.of(new KafkaPart(KafkaType.Long))));
        assertThat(schemas.value(), is(new KafkaPart(KafkaType.String)));
    }

    @Test
    void shouldExtractSchemaRefInfo() throws Exception {
        // Given:
        final String json =
                "bindings:\n"
                        + "  kafka:\n"
                        + "    key:\n"
                        + "      $ref: key.schema\n"
                        + "payload:\n"
                        + "  $ref: value.schema";

        final Message message = MAPPER.readValue(json, Message.class);

        // When:
        final SchemaInfo schemas = message.schemas();

        // Then:
        assertThat(schemas.key(), is(Optional.of(new SchemaRefPart("key.schema"))));
        assertThat(schemas.value(), is(new SchemaRefPart("value.schema")));
    }

    @Test
    void shouldNotBaulkAtInlineSchema() throws Exception {
        // Given:
        final String json =
                "bindings:\n"
                        + "  kafka:\n"
                        + "    key:\n"
                        + "      type: object\n"
                        + "      properties:\n"
                        + "        id:\n"
                        + "payload:\n"
                        + "  type: object\n"
                        + "  properties:\n"
                        + "    name:\n";

        final Message message = MAPPER.readValue(json, Message.class);

        // When:
        final SchemaInfo schemas = message.schemas();

        // Then:
        assertThat(schemas.key().map(Object::getClass), is(Optional.of(OtherPart.class)));
        assertThat(schemas.value(), is(instanceOf(OtherPart.class)));
    }

    @Test
    void shouldExtractSchemaFormat() throws Exception {
        // Given:
        final String json =
                "bindings:\n"
                        + "  kafka:\n"
                        + "    key:\n"
                        + "      type: long\n"
                        + "payload:\n"
                        + "  $ref: value.schema\n"
                        + "schemaFormat: expected\n";

        final Message message = MAPPER.readValue(json, Message.class);

        // When:
        final SchemaInfo schemas = message.schemas();

        // Then:
        assertThat(schemas.schemaFormat(), is(Optional.of("expected")));
    }

    @Test
    void shouldExtractSchemaIdLocation() throws Exception {
        // Given:
        final String json =
                "bindings:\n"
                        + "  kafka:\n"
                        + "    key:\n"
                        + "      type: long\n"
                        + "    schemaIdLocation: expected\n"
                        + "payload:\n"
                        + "  $ref: value.schema";

        final Message message = MAPPER.readValue(json, Message.class);

        // When:
        final SchemaInfo schemas = message.schemas();

        // Then:
        assertThat(schemas.schemaIdLocation(), is(Optional.of("expected")));
    }

    @Test
    void shouldExtractContentType() throws Exception {
        // Given:
        final String json =
                "bindings:\n"
                        + "  kafka:\n"
                        + "    key:\n"
                        + "      type: long\n"
                        + "payload:\n"
                        + "  $ref: value.schema\n"
                        + "contentType: expected\n";

        final Message message = MAPPER.readValue(json, Message.class);

        // When:
        final SchemaInfo schemas = message.schemas();

        // Then:
        assertThat(schemas.contentType(), is(Optional.of("expected")));
    }

    @Test
    void shouldExtractSchemaLookupStrategy() throws Exception {
        // Given:
        final String json =
                "bindings:\n"
                        + "  kafka:\n"
                        + "    key:\n"
                        + "      type: long\n"
                        + "    schemaLookupStrategy: expected\n"
                        + "payload:\n"
                        + "  $ref: value.schema";

        final Message message = MAPPER.readValue(json, Message.class);

        // When:
        final SchemaInfo schemas = message.schemas();

        // Then:
        assertThat(schemas.schemaLookupStrategy(), is(Optional.of("expected")));
    }
}
