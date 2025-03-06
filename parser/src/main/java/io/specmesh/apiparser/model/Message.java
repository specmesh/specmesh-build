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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.specmesh.apiparser.AsyncApiParser.APIParserException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;

/**
 * Pojo representing a Message.
 *
 * @see <a href= "https://www.asyncapi.com/docs/reference/specification/v2.4.0#messageObject">spec
 *     docs</a>
 */
@Builder
@Value
@Accessors(fluent = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressFBWarnings
@SuppressWarnings({"unchecked", "rawtypes"})
public final class Message {
    @JsonProperty private String messageId;

    @JsonProperty private Map headers;

    @JsonProperty private RecordPart payload;

    @JsonProperty private Map correlationId;

    @JsonProperty private String schemaFormat;

    @JsonProperty private String contentType;

    @JsonProperty private String name;

    @JsonProperty private String title;

    @JsonProperty private String summary;

    @JsonProperty private String description;

    @JsonProperty private List<Tag> tags;

    @JsonProperty private Bindings bindings;

    @JsonProperty private Map traits;

    private Message() {
        this.headers = Map.of();
        this.correlationId = Map.of();
        this.schemaFormat = "";
        this.contentType = "";
        this.tags = List.of();
        this.traits = Map.of();
        this.messageId = null;
        this.payload = null;
        this.name = null;
        this.title = null;
        this.summary = null;
        this.description = null;
        this.bindings = null;
    }

    /**
     * @return The schemas in use
     */
    public SchemaInfo schemas() {
        if (payload == null) {
            throw new APIParserException("missing 'message.payload' definition");
        }

        final Optional<KafkaBinding> kafkaBinding =
                Optional.ofNullable(bindings).map(Bindings::kafka);

        return new SchemaInfo(
                kafkaBinding.flatMap(KafkaBinding::key),
                payload,
                Optional.ofNullable(schemaFormat),
                kafkaBinding.map(KafkaBinding::schemaIdLocation),
                Optional.ofNullable(contentType),
                kafkaBinding.map(KafkaBinding::schemaLookupStrategy));
    }
}
