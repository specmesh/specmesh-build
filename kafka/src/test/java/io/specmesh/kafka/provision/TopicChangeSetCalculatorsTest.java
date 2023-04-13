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
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import io.specmesh.kafka.provision.TopicProvisioner.Topic;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.Test;

class TopicChangeSetCalculatorsTest {

    final TopicChangeSetCalculators.UpdateChangeSetCalculator calculator =
            new TopicChangeSetCalculators.UpdateChangeSetCalculator();

    @Test
    void shouldIgnorePartitionDeltaWhenSmaller() {

        final var existing = List.of(Topic.builder().name("test").partitions(3).build());
        final var required = List.of(Topic.builder().name("test").partitions(1).build());
        final var results = calculator.calculate(existing, required);
        assertThat(results, is(hasSize(0)));
    }

    @Test
    void shouldDetectPartitionDeltaWhenLarger() {

        final var existing = List.of(Topic.builder().name("test").partitions(1).build());
        final var required = List.of(Topic.builder().name("test").partitions(3).build());
        final var results = calculator.calculate(existing, required);
        assertThat(results, is(hasSize(1)));
        final var updatedTopic = results.iterator().next();
        assertThat(updatedTopic.state(), is(Status.STATE.UPDATE));
        assertThat(updatedTopic.partitions(), is(3));
        assertThat(updatedTopic.name(), is("test"));
    }

    @Test
    void shouldDetectRetentionChange() {

        final var existing =
                List.of(
                        Topic.builder()
                                .name("test")
                                .partitions(1)
                                .config(Map.of(TopicConfig.RETENTION_MS_CONFIG, "100"))
                                .build());
        final var required =
                List.of(
                        Topic.builder()
                                .name("test")
                                .partitions(1)
                                .config(Map.of(TopicConfig.RETENTION_MS_CONFIG, "200"))
                                .build());
        final var results = calculator.calculate(existing, required);
        assertThat(results, is(hasSize(1)));
        final var updatedTopic = results.iterator().next();
        assertThat(updatedTopic.state(), is(Status.STATE.UPDATE));
        assertThat(updatedTopic.config().get(TopicConfig.RETENTION_MS_CONFIG), is("200"));
    }
}
