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

package io.specmesh.apiparser;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import io.specmesh.apiparser.model.ApiSpec;
import io.specmesh.apiparser.model.Bindings;
import io.specmesh.apiparser.model.Channel;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.config.TopicConfig;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

public class AsyncApiParserTest {
    private static final ApiSpec API_SPEC =
            TestSpecLoader.loadFromClassPath("test-streetlights-simple-api.yaml");

    @Test
    public void shouldReturnRootIdWithDelimiter() {
        assertThat(API_SPEC.id(), is("simple.streetlights"));
    }

    @Test
    public void shouldReturnCanonicalChannelNames() {
        final Map<String, Channel> channelsMap = API_SPEC.channels();
        assertThat(channelsMap.entrySet(), hasSize(2));
        assertThat(
                "Should have assembled id + channelname",
                channelsMap.keySet(),
                hasItem("simple.streetlights.public.light.measured"));
        assertThat(
                "Should use absolute path",
                channelsMap.keySet(),
                hasItem("london.hammersmith.transport.public.tube"));
    }

    @Test
    public void shouldReturnKafkaBindingsForCreation() {
        final Map<String, Channel> channelsMap = API_SPEC.channels();
        assertThat(channelsMap.entrySet(), hasSize(2));
        assertThat(
                "Should have assembled id + channelname",
                channelsMap.keySet(),
                hasItem("simple.streetlights.public.light.measured"));

        final Bindings producerBindings =
                channelsMap.get("simple.streetlights.public.light.measured").bindings();
        assertThat(producerBindings.kafka().partitions(), is(3));
        assertThat(producerBindings.kafka().replicas(), is(1));

        assertThat(
                "Should use absolute path",
                channelsMap.keySet(),
                hasItem("london.hammersmith.transport.public.tube"));
    }

    @Test
    public void shouldReturnOperationTags() {
        final Map<String, Channel> channelsMap = API_SPEC.channels();
        assertThat(channelsMap.entrySet(), hasSize(2));
        assertThat(
                "Should have assembled 'id + channelname'",
                channelsMap.keySet(),
                hasItem("simple.streetlights.public.light.measured"));

        final Bindings producerBindings =
                channelsMap.get("simple.streetlights.public.light.measured").bindings();
        assertThat(producerBindings.kafka().configs().entrySet(), hasSize(2));
        assertThat(
                producerBindings.kafka().configs().keySet(),
                is(Set.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.RETENTION_MS_CONFIG)));
        assertThat(producerBindings.kafka().envs(), hasSize(2));

        assertThat(
                channelsMap.values().iterator().next().publish().message().tags(),
                Matchers.hasSize(2));
    }

    @Test
    public void shouldReturnProducerMessageSchema() {
        final Map<String, Channel> channelsMap = API_SPEC.channels();
        assertThat(channelsMap.entrySet(), hasSize(2));
        assertThat(
                "Should have assembled 'id + channelname'",
                channelsMap.keySet(),
                hasItem("simple.streetlights.public.light.measured"));

        final Bindings producerBindings =
                channelsMap.get("simple.streetlights.public.light.measured").bindings();
        assertThat(producerBindings.kafka().configs().entrySet(), hasSize(2));
        assertThat(
                producerBindings.kafka().configs().keySet(),
                is(Set.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.RETENTION_MS_CONFIG)));
        assertThat(producerBindings.kafka().envs(), hasSize(2));

        assertThat(
                channelsMap.values().iterator().next().publish().message().tags(),
                Matchers.hasSize(2));
    }
}
