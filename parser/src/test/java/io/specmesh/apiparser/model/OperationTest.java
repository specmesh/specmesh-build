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
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.databind.json.JsonMapper;
import io.specmesh.apiparser.parse.SpecMapper;
import org.junit.jupiter.api.Test;

class OperationTest {

    private static final JsonMapper MAPPER = SpecMapper.mapper();

    @Test
    void shouldParseMinimal() throws Exception {
        // Given:
        final String yaml = "operationId: something";

        // When:
        final Operation binding = MAPPER.readValue(yaml, Operation.class);

        // Then:
        assertThat(binding, is(Operation.builder().operationId("something").build()));
    }

    @Test
    void shouldThrowWhenParsingIfMissingOperationId() {
        // Given:
        final String yaml = "summary: something";

        // When:
        final Exception e =
                assertThrows(Exception.class, () -> MAPPER.readValue(yaml, Operation.class));

        // Then:
        assertThat(e.getMessage(), containsString("operationId"));
    }

    @Test
    void shouldThrowFromBuilderIfMissingOperationId() {
        // When:
        final Exception e = assertThrows(Exception.class, () -> Operation.builder().build());

        // Then:
        assertThat(e.getMessage(), containsString("operationId"));
    }
}
