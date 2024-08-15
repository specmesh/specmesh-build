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
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.databind.json.JsonMapper;
import io.specmesh.apiparser.parse.SpecMapper;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class KafkaBindingTest {

    private static final JsonMapper MAPPER = SpecMapper.mapper();

    @Test
    void shouldParseMinimal() throws Exception {
        // Given:
        final String yaml = "configs: {}";

        // When:
        final KafkaBinding binding = MAPPER.readValue(yaml, KafkaBinding.class);

        // Then:
        assertThat(binding, is(KafkaBinding.builder().build()));
    }

    @Test
    void shouldParsePartitions() throws Exception {
        // Given:
        final String yaml = "partitions: 4";

        // When:
        final KafkaBinding binding = MAPPER.readValue(yaml, KafkaBinding.class);

        // Then:
        assertThat(binding.partitions(), is(Optional.of(4)));
    }

    @Test
    void shouldParseReplicationFactor() throws Exception {
        // Given:
        final String yaml = "replicas: 2";

        // When:
        final KafkaBinding binding = MAPPER.readValue(yaml, KafkaBinding.class);

        // Then:
        assertThat(binding.replicas(), is(Optional.of((short) 2)));
    }
}
