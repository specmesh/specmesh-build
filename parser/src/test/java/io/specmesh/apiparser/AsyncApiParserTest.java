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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.fail;

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

    private static final ApiSpec API_SPEC_KEY_VALUE =
            TestSpecLoader.loadFromClassPath("keyvalue_schema_demo-api.yaml");

    @Test
    public void shouldReturnRootIdWithDelimiter() {
        assertThat(API_SPEC.id(), is(".simple.streetlights"));
    }

    @Test
    public void shouldReturnCanonicalChannelNames() {
        final Map<String, Channel> channelsMap = API_SPEC.channels();
        assertThat(channelsMap.entrySet(), hasSize(2));
        assertThat(
                "Should have assembled id + channelname",
                channelsMap.keySet(),
                hasItem(".simple.streetlights._public.light.measured"));
        assertThat(
                "Should use absolute path",
                channelsMap.keySet(),
                hasItem("london.hammersmith.transport._public.tube"));
    }

    @Test
    public void shouldReturnKafkaBindingsForCreation() {
        final Map<String, Channel> channelsMap = API_SPEC.channels();
        assertThat(channelsMap.entrySet(), hasSize(2));
        assertThat(
                "Should have assembled id + channelname",
                channelsMap.keySet(),
                hasItem(".simple.streetlights._public.light.measured"));

        final Bindings producerBindings =
                channelsMap.get(".simple.streetlights._public.light.measured").bindings();
        assertThat(producerBindings.kafka().partitions(), is(3));
        assertThat(producerBindings.kafka().replicas(), is(1));

        assertThat(
                "Should use absolute path",
                channelsMap.keySet(),
                hasItem("london.hammersmith.transport._public.tube"));
    }

    @Test
    public void shouldReturnOperationTags() {
        final Map<String, Channel> channelsMap = API_SPEC.channels();
        assertThat(channelsMap.entrySet(), hasSize(2));
        assertThat(
                "Should have assembled 'id + channelname'",
                channelsMap.keySet(),
                hasItem(".simple.streetlights._public.light.measured"));

        final Bindings producerBindings =
                channelsMap.get(".simple.streetlights._public.light.measured").bindings();
        assertThat(producerBindings.kafka().configs().entrySet(), hasSize(1));
        assertThat(
                producerBindings.kafka().configs().keySet(),
                is(Set.of(TopicConfig.CLEANUP_POLICY_CONFIG)));
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
                hasItem(".simple.streetlights._public.light.measured"));

        final Bindings producerBindings =
                channelsMap.get(".simple.streetlights._public.light.measured").bindings();
        assertThat(producerBindings.kafka().configs().entrySet(), hasSize(1));
        assertThat(
                producerBindings.kafka().configs().keySet(),
                is(Set.of(TopicConfig.CLEANUP_POLICY_CONFIG)));
        assertThat(producerBindings.kafka().envs(), hasSize(2));

        assertThat(
                channelsMap.values().iterator().next().publish().message().tags(),
                Matchers.hasSize(2));
    }

    @Test
    public void shouldSupportOldSchemaRefLookup() {
        final var channels = API_SPEC_KEY_VALUE.channels();

        final var oldStyleChannel = channels.get("simple.schema-demo._public.example_old");
        oldStyleChannel.publish().message().schemaRef();

        try {
            oldStyleChannel.publish().message().keyType();
            fail("should have failed on KeyLookup");
        } catch (Exception ex) {

        }
        // throw not supports
        try {
            oldStyleChannel.publish().message().valueType();
            fail("should have failed on ValueLookup");
        } catch (Exception ex) {
        }
    }

    @Test
    public void shouldSupportPrimitiveKeyAndValue() {
        final var channels = API_SPEC_KEY_VALUE.channels();

        final var oldStyleChannel = channels.get("simple.schema-demo._public.example_primitive");

        try {
            oldStyleChannel.publish().message().schemaRef();
            fail("should have failed on SchemaRefLookup");
        } catch (Exception ex) {
        }

        final var primitiveKeyType = oldStyleChannel.publish().message().keyType();
        assertThat(primitiveKeyType, is("string"));
        final var primitiveValueType = oldStyleChannel.publish().message().valueType();
        assertThat(primitiveValueType, is("double"));
    }

    @Test
    public void shouldSupportSchemaKeyAndValue() {
        final var channels = API_SPEC_KEY_VALUE.channels();

        final var channel = channels.get("simple.schema-demo._public.example_kv_schema");

        try {
            channel.publish().message().schemaRef();
            fail("should have failed on SchemaRefLookup");
        } catch (Exception ex) {
        }

        final var primitiveKeyType = channel.publish().message().keyType();
        assertThat(primitiveKeyType, is("KeyValue.json"));
        final var primitiveValueType = channel.publish().message().valueType();
        assertThat(primitiveValueType, is("MessageValue.avsc"));
    }

    @Test
    public void shouldSupportRestrictedSetOfPrimitiveTypes() {
        final var channels = API_SPEC_KEY_VALUE.channels();
        final var channel = channels.get("simple.schema-demo._public.example_unsupported");

        try {
            channel.publish().message().keyType();
            fail("should have failed on unsupported type");
        } catch (Exception ex) {
            assertThat(ex.getMessage(), containsString("unsupported-type-thing"));
            assertThat(ex.getMessage(), containsString("Unsupported type"));
        }
    }
}
