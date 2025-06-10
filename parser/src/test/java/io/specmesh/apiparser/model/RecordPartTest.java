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

import com.fasterxml.jackson.databind.json.JsonMapper;
import io.specmesh.apiparser.AsyncApiParser.APIParserException;
import io.specmesh.apiparser.model.RecordPart.KafkaType;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class RecordPartTest {
    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    @Test
    void shouldThrowIfNotObject() {
        // Given:
        final String json = "1";

        // When:
        final Exception e =
                assertThrows(
                        APIParserException.class, () -> MAPPER.readValue(json, RecordPart.class));

        // Then:
        assertThat(
                e.getMessage(),
                startsWith("Record part should be an object with either $ref or type."));
    }

    @Test
    void shouldThrowIfArray() {
        // Given:
        final String json = "[]";

        // When:
        final Exception e =
                assertThrows(
                        APIParserException.class, () -> MAPPER.readValue(json, RecordPart.class));

        // Then:
        assertThat(
                e.getMessage(),
                startsWith("Record part should be an object with either $ref or type."));
    }

    @Test
    void shouldThrowIfEmptyObject() {
        // Given:
        final String json = "{}";

        // When:
        final Exception e =
                assertThrows(
                        APIParserException.class, () -> MAPPER.readValue(json, RecordPart.class));

        // Then:
        assertThat(
                e.getMessage(),
                startsWith("Record part requires either $ref or type, but not both"));
    }

    @Test
    void shouldThrowOnObjectWithoutProps() {
        // Given:
        final String json = "{\"type\": \"object\"}";

        // When:
        final Exception e =
                assertThrows(
                        APIParserException.class, () -> MAPPER.readValue(json, RecordPart.class));

        // Then:
        assertThat(e.getMessage(), startsWith("'object' types requires inline 'properties."));
    }

    @Test
    void shouldThrowIfBothRefAndTypeDefined() {
        // Given:
        final String json = "{" + "\"type\": \"string\"," + "\"$ref\": \"/some/path\"" + "}";

        // When:
        final Exception e =
                assertThrows(
                        APIParserException.class, () -> MAPPER.readValue(json, RecordPart.class));

        // Then:
        assertThat(
                e.getMessage(),
                startsWith(
                        "Record part requires either $ref or type, but not both. location:"
                                + " unknown"));
    }

    @Test
    void shouldThrowOnUnknownKafkaType() {
        // Given:
        final String json = "{\"type\": \"not-valid\"}";

        // When:
        final Exception e =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> MAPPER.readValue(json, RecordPart.class));

        // Then:
        assertThat(
                e.getMessage(),
                containsString(
                        "Unknown KafkaType: not-valid. Valid values are: [uuid, boolean, long, int,"
                                + " short, float, double, string, bytes, void]"));
    }

    @ParameterizedTest
    @EnumSource(KafkaType.class)
    void shouldDeserializeKafkaType(final KafkaType keyType) throws Exception {
        // Given:
        final String json = "{\"type\": \"" + keyType.toString() + "\"}";

        // When:
        final RecordPart result = MAPPER.readValue(json, RecordPart.class);

        // Then:
        assertThat(result, is(new RecordPart.KafkaPart(keyType)));
        assertThat(result.schemaRef(), is(Optional.empty()));
    }

    @Test
    void shouldDeserializeSchemaRef() throws Exception {
        // Given:
        final String json = "{\"$ref\": \"/some/path/to/schema.avsc\"}";

        // When:
        final RecordPart result = MAPPER.readValue(json, RecordPart.class);

        // Then:
        assertThat(result.schemaRef(), is(Optional.of("/some/path/to/schema.avsc")));
    }

    @Test
    void shouldDeserializeInlineSchema() throws Exception {
        // Given:
        final String json =
                "{"
                        + "\"type\": \"object\","
                        + "\"properties\": {"
                        + "  \"name\": {"
                        + "    \"type\": \"string\""
                        + "  }"
                        + " }"
                        + "}";

        // When:
        final RecordPart result = MAPPER.readValue(json, RecordPart.class);

        // Then:
        assertThat(result, is(instanceOf(RecordPart.OtherPart.class)));
        assertThat(result.schemaRef(), is(Optional.empty()));
    }

    @ParameterizedTest
    @EnumSource(KafkaType.class)
    void shouldHaveSerializerForEachKafkaType(final KafkaType keyType) throws Exception {
        // Given:
        final String typeName =
                keyType == KafkaType.Int
                        ? "Integer"
                        : keyType.name().substring(0, 1).toUpperCase()
                                + keyType.name().substring(1);
        final String serializerClassName =
                "org.apache.kafka.common.serialization." + typeName + "Serializer";

        // When:
        getClass().getClassLoader().loadClass(serializerClassName);

        // Then: did not throw.
    }
}
