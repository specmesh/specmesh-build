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
import static org.hamcrest.Matchers.is;

import io.specmesh.apiparser.model.ApiSpec;
import io.specmesh.apiparser.model.Channel;
import io.specmesh.apiparser.model.Operation;
import io.specmesh.apiparser.model.RecordPart;
import io.specmesh.apiparser.model.RecordPart.KafkaPart;
import io.specmesh.apiparser.model.RecordPart.SchemaRefPart;
import io.specmesh.apiparser.model.SchemaInfo;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class AsyncApiSchemaParserTest {
    private static final ApiSpec API_SPEC =
            TestSpecLoader.loadFromClassPath("parser_simple_schema_demo-api.yaml");

    @Test
    public void shouldReturnProducerMessageSchema() {
        final Map<String, Channel> channelsMap = API_SPEC.channels();
        assertThat(channelsMap.keySet(), hasItem(".simple.schema-demo._public.user.signed"));

        final Operation op = channelsMap.get(".simple.schema-demo._public.user.signed").publish();

        assertThat(
                op.message().schemaFormat(), is("application/vnd.apache.avro+json;version=1.9.0"));
        assertThat(op.message().contentType(), is("application/octet-stream"));
        assertThat(op.message().bindings().kafka().schemaIdLocation(), is("payload"));
        assertThat(
                op.message().bindings().kafka().key(),
                is(Optional.of(new SchemaRefPart("key.avsc"))));
        assertThat(
                op.message().payload().schemaRef(),
                is(Optional.of("simple_schema_demo_user-signedup.avsc")));

        assertThat(
                op.schemaInfo().flatMap(SchemaInfo::schemaFormat),
                is(Optional.of(op.message().schemaFormat())));
        assertThat(
                op.schemaInfo().flatMap(SchemaInfo::contentType),
                is(Optional.of(op.message().contentType())));
        assertThat(
                op.schemaInfo().flatMap(SchemaInfo::schemaIdLocation),
                is(Optional.of(op.message().bindings().kafka().schemaIdLocation())));
        assertThat(
                op.schemaInfo().flatMap(SchemaInfo::key),
                is(op.message().bindings().kafka().key()));
        assertThat(op.schemaInfo().map(SchemaInfo::value), is(Optional.of(op.message().payload())));
    }

    @Test
    public void shouldReturnSubscriberMessageSchema() {
        // Given:
        final Map<String, Channel> channelsMap = API_SPEC.channels();
        assertThat(channelsMap.keySet(), hasItem("london.hammersmith.transport._public.tube"));
        // When:
        final Operation op =
                channelsMap.get("london.hammersmith.transport._public.tube").subscribe();

        // Then:
        assertThat(
                op.message().schemaFormat(), is("application/vnd.apache.avro+json;version=1.9.0"));
        assertThat(op.message().contentType(), is("application/octet-stream"));
        assertThat(op.message().bindings().kafka().schemaIdLocation(), is("header"));
        assertThat(
                op.message().bindings().kafka().key(),
                is(Optional.of(new KafkaPart(RecordPart.KafkaType.String))));
        assertThat(
                op.message().payload().schemaRef(),
                is(Optional.of("london_hammersmith_transport_public_passenger.avsc")));

        assertThat(
                op.schemaInfo().flatMap(SchemaInfo::schemaFormat),
                is(Optional.of(op.message().schemaFormat())));
        assertThat(
                op.schemaInfo().flatMap(SchemaInfo::contentType),
                is(Optional.of(op.message().contentType())));
        assertThat(
                op.schemaInfo().flatMap(SchemaInfo::schemaIdLocation),
                is(Optional.of(op.message().bindings().kafka().schemaIdLocation())));
        assertThat(
                op.schemaInfo().flatMap(SchemaInfo::key),
                is(op.message().bindings().kafka().key()));
        assertThat(op.schemaInfo().map(SchemaInfo::value), is(Optional.of(op.message().payload())));
    }

    @Test
    void shouldNotBlowUpIfNotMessageDefined() {
        // Given:
        final Map<String, Channel> channelsMap = API_SPEC.channels();
        assertThat(channelsMap.keySet(), hasItem(".simple.schema-demo._public.user.message-less"));
        final Operation op =
                channelsMap.get(".simple.schema-demo._public.user.message-less").publish();

        // Then:
        assertThat(op.schemaInfo(), is(Optional.empty()));
    }
}
