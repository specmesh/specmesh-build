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

package io.specmesh.apiparser.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JsonLocationTest {

    private static final ObjectMapper MAPPER =
            new ObjectMapper().enable(JsonParser.Feature.INCLUDE_SOURCE_IN_LOCATION);

    @TempDir private Path testDir;

    @Test
    void shouldGetLocationIfParsingFile() throws Exception {
        // Given:
        final String json = "{\"k\": \"v\"}";
        final Path file = testDir.resolve("test.yml");
        Files.writeString(file, json);

        // When:
        final TestThing result = MAPPER.readValue(file.toFile(), TestThing.class);

        // Then:
        final String location = result.location.toString();
        assertThat(location, startsWith("file:///"));
        assertThat(location, endsWith("/test.yml:1"));
    }

    @Test
    void shouldNotGetLocationIfParsingText() throws Exception {
        // Given:
        final String json = "{\"k\": \"v\"}";

        // When:
        final TestThing result = MAPPER.readValue(json, TestThing.class);

        // Then:
        assertThat(result.location, is(URI.create("unknown")));
    }

    @JsonDeserialize(using = JsonLocationTest.ThingDeserializer.class)
    public static final class TestThing {

        private final URI location;

        TestThing(final URI location) {
            this.location = location;
        }
    }

    public static final class ThingDeserializer extends JsonDeserializer<TestThing> {
        @Override
        public TestThing deserialize(final JsonParser p, final DeserializationContext ctx)
                throws IOException {
            p.nextFieldName();
            final URI location = JsonLocation.location(p);
            p.nextTextValue();
            return new TestThing(location);
        }
    }
}
