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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import io.specmesh.apiparser.model.ApiSpec;
import io.specmesh.apiparser.model.Bindings;
import io.specmesh.apiparser.model.Channel;
import io.specmesh.apiparser.model.Payload;
import io.specmesh.apiparser.model.Payload.KafkaPart;
import io.specmesh.apiparser.model.Payload.KafkaPart.KafkaType;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.Test;

public class AsyncApiParserTest {
    private static final ApiSpec API_SPEC =
            TestSpecLoader.loadFromClassPath("test-streetlights-simple-api.yaml");

    @Test
    public void shouldReturnRootIdWithDelimiter() {
        assertThat(API_SPEC.id(), is(".simple.streetlights"));
    }

    @Test
    public void shouldReturnCanonicalChannelNames() {
        assertThat(
                API_SPEC.channels().keySet(),
                containsInAnyOrder(
                        ".simple.streetlights._public.light.measured",
                        "london.hammersmith.transport._public.tube",
                        ".simple.streetlights._public.avro.key",
                        ".simple.streetlights._public.avro.value"));
    }

    @Test
    public void shouldReturnKafkaBindingsForCreation() {
        final Bindings producerBindings =
                API_SPEC.channels().get(".simple.streetlights._public.light.measured").bindings();
        assertThat(producerBindings.kafka().partitions(), is(3));
        assertThat(producerBindings.kafka().replicas(), is(1));
    }

    @Test
    public void shouldReturnOperationTags() {
        final Bindings producerBindings =
                API_SPEC.channels().get(".simple.streetlights._public.light.measured").bindings();

        assertThat(
                producerBindings.kafka().configs(),
                hasEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE));
        assertThat(producerBindings.kafka().envs(), hasSize(2));
    }

    @Test
    public void shouldReturnProducerMessageSchema() {
        final Bindings producerBindings =
                API_SPEC.channels().get(".simple.streetlights._public.light.measured").bindings();

        assertThat(producerBindings.kafka().configs().entrySet(), hasSize(1));
        assertThat(
                producerBindings.kafka().configs().keySet(),
                is(Set.of(TopicConfig.CLEANUP_POLICY_CONFIG)));
        assertThat(producerBindings.kafka().envs(), hasSize(2));
    }

    @Test
    public void shouldSupportSimpleKey() {
        final Channel channel =
                API_SPEC.channels().get(".simple.streetlights._public.light.measured");

        final Payload payload = channel.publish().message().payload();

        assertThat(payload.key(), is(new KafkaPart(KafkaType.String)));
    }

    @Test
    public void shouldSupportSchemaRefKey() {
        final Channel channel = API_SPEC.channels().get(".simple.streetlights._public.avro.key");

        final Payload payload = channel.publish().message().payload();

        assertThat(
                payload.key().schemaRef(),
                is(Optional.of("/schema/com.example.shared.Currency.avsc")));
    }

    @Test
    public void shouldSupportSimpleValue() {
        final Channel channel = API_SPEC.channels().get(".simple.streetlights._public.avro.key");

        final Payload payload = channel.publish().message().payload();

        assertThat(payload.value(), is(new KafkaPart(KafkaType.Int)));
    }

    @Test
    public void shouldSupportSchemaRefValue() {
        final Channel channel = API_SPEC.channels().get(".simple.streetlights._public.avro.value");

        final Payload payload = channel.publish().message().payload();

        assertThat(
                payload.value().schemaRef(),
                is(Optional.of("/schema/com.example.shared.Currency.avsc")));
    }
}
