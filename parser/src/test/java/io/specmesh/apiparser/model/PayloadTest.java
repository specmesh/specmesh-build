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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.specmesh.apiparser.model.Payload.KafkaPart.KafkaType;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class PayloadTest {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    @Test
    void shouldThrowIfNoKey() {
        // Given:
        final String json =
                "{\n" + "  \"value\": {\n" + "    \"type\": \"string\"\n" + "  }\n" + "}";

        // When:
        final Exception e =
                assertThrows(
                        JsonMappingException.class, () -> MAPPER.readValue(json, Payload.class));

        // Then:
        assertThat(e.getMessage(), startsWith("Missing required creator property 'key'"));
    }

    @Test
    void shouldThrowIfNoValue() {
        // Given:
        final String json = "{\n" + "  \"key\": {\n" + "    \"type\": \"string\"\n" + "  }" + "}";

        // When:
        final Exception e =
                assertThrows(
                        JsonMappingException.class, () -> MAPPER.readValue(json, Payload.class));

        // Then:
        assertThat(e.getMessage(), startsWith("Missing required creator property 'value'"));
    }

    @Test
    void shouldThrowIfBothRefAndTypeDefined() {
        // Given:
        final String json =
                "{\n"
                        + "  \"key\": {\n"
                        + "    \"type\": \"string\",\n"
                        + "    \"$ref\": \"/some/path\"\n"
                        + "  },\n"
                        + "  \"value\": {\n"
                        + "    \"type\": \"string\"\n"
                        + "  }\n"
                        + "}";

        // When:
        final Exception e =
                assertThrows(
                        JsonMappingException.class, () -> MAPPER.readValue(json, Payload.class));

        // Then:
        assertThat(
                e.getMessage(),
                startsWith(
                        "Payload part requires either $ref or type, but not both. location:"
                                + " unknown"));
    }

    @Test
    void shouldThrowOnUnknownKafkaType() {
        // Given:
        final String json =
                "{\n"
                        + "  \"key\": {\n"
                        + "    \"type\": \"not-valid\"\n"
                        + "  },\n"
                        + "  \"value\": {\n"
                        + "    \"type\": \"string\"\n"
                        + "  }\n"
                        + "}";

        // When:
        final Exception e =
                assertThrows(
                        JsonMappingException.class, () -> MAPPER.readValue(json, Payload.class));

        // Then:
        assertThat(
                e.getMessage(),
                containsString(
                        "Unknown KafkaType: not-valid. Valid values are: "
                                + "[uuid, long, int, short, float, double, string, bytes, void]"));
    }

    @Test
    void shouldThrowOnPrimitivePart() {
        // Given:
        final String json =
                "{\n"
                        + "  \"key\": {\n"
                        + "    \"type\": \"int\"\n"
                        + "  },\n"
                        + "  \"value\": 10\n"
                        + "  }\n"
                        + "}";

        // When:
        final Exception e =
                assertThrows(
                        JsonMappingException.class, () -> MAPPER.readValue(json, Payload.class));

        // Then:
        assertThat(
                e.getMessage(),
                containsString(
                        "Payload part should be an object with either $ref or type, but not both"));
    }

    @Test
    void shouldThrowOnArrayPart() {
        // Given:
        final String json =
                "{\n"
                        + "  \"key\": {\n"
                        + "    \"type\": \"int\"\n"
                        + "  },\n"
                        + "  \"value\": [ \"type\", \"int\" ]\n"
                        + "}";

        // When:
        final Exception e =
                assertThrows(
                        JsonMappingException.class, () -> MAPPER.readValue(json, Payload.class));

        // Then:
        assertThat(
                e.getMessage(),
                containsString(
                        "Payload part should be an object with either $ref or type, but not both"));
    }

    @ParameterizedTest
    @EnumSource(KafkaType.class)
    void shouldDeserializeKafkaTypeKey(final KafkaType keyType) throws Exception {
        // Given:
        final String json =
                "{\n"
                        + "  \"key\": {\n"
                        + "    \"type\": \""
                        + keyType.toString()
                        + "\"\n"
                        + "  },\n"
                        + "  \"value\": {\n"
                        + "    \"type\": \"string\"\n"
                        + "  }\n"
                        + "}";

        // When:
        final Payload result = MAPPER.readValue(json, Payload.class);

        // Then:
        assertThat(result.key(), is(new Payload.KafkaPart(keyType)));
    }

    @ParameterizedTest
    @EnumSource(KafkaType.class)
    void shouldDeserializeKafkaTypeValue(final KafkaType valueType) throws Exception {
        // Given:
        final String json =
                "{\n"
                        + "  \"key\": {\n"
                        + "    \"type\": \"string\"\n"
                        + "  },\n"
                        + "  \"value\": {\n"
                        + "    \"type\": \""
                        + valueType.toString()
                        + "\"\n"
                        + "  }\n"
                        + "}";

        // When:
        final Payload result = MAPPER.readValue(json, Payload.class);

        // Then:
        assertThat(result.value(), is(new Payload.KafkaPart(valueType)));
    }

    @Test
    void shouldDeserializeSchemaRef() throws Exception {
        // Given:
        final String json =
                "{\n"
                        + "  \"key\": {\n"
                        + "    \"$ref\": \"/some/path/key.avsc\"\n"
                        + "  },\n"
                        + "  \"value\": {\n"
                        + "    \"$ref\": \"/some/path/value.avsc\"\n"
                        + "  }\n"
                        + "}";

        // When:
        final Payload result = MAPPER.readValue(json, Payload.class);

        // Then:
        assertThat(result.key().schemaRef(), is(Optional.of("/some/path/key.avsc")));
        assertThat(result.value().schemaRef(), is(Optional.of("/some/path/value.avsc")));
    }

    @Test
    void shouldDeserializeInlineSchema() throws Exception {
        // Given:
        final String json =
                "{\n"
                        + "  \"key\": {\n"
                        + "    \"type\": \"object\",\n"
                        + "    \"properties\": {\n"
                        + "      \"name\": {\n"
                        + "        \"type\": \"string\"\n"
                        + "      }\n"
                        + "    }\n"
                        + "  },\n"
                        + "  \"value\": {\n"
                        + "    \"properties\": {\n"
                        + "      \"id\": {\n"
                        + "        \"type\": \"integer\"\n"
                        + "      }\n"
                        + "    },\n"
                        + "    \"type\": \"object\"\n"
                        + "  }\n"
                        + "}";

        // When:
        final Payload payload = MAPPER.readValue(json, Payload.class);

        // Then:
        assertThat(payload.key(), is(instanceOf(Payload.OtherPart.class)));
        assertThat(payload.value(), is(instanceOf(Payload.OtherPart.class)));
        assertThat(payload.key().schemaRef(), is(Optional.empty()));
        assertThat(payload.value().schemaRef(), is(Optional.empty()));
    }
}
