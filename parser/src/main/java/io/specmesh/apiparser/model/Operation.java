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

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.specmesh.apiparser.AsyncApiParser.APIParserException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

/**
 * Pojo representing a Message.
 *
 * @see <a href=
 *     "https://www.asyncapi.com/docs/reference/specification/v2.4.0#operationObject">operationObject</a>
 * @see <a href=
 *     "https://www.asyncapi.com/docs/reference/specification/v2.4.0#messageTraitObject">messageTraitObject</a>
 */
@Builder
@Data
@Accessors(fluent = true)
@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressFBWarnings
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@SuppressWarnings("rawtypes")
public final class Operation {
    @EqualsAndHashCode.Include @JsonProperty private String operationId;

    @JsonProperty private String summary;

    @JsonProperty private String description;

    @JsonProperty @Builder.Default private List<Tag> tags = List.of();

    @JsonProperty private Bindings bindings;

    // https://www.asyncapi.com/docs/reference/specification/v2.4.0#operationTraitObject
    @JsonProperty private Map traits;

    @JsonProperty private Message message;

    @JsonCreator
    private Operation(
            @JsonProperty(required = true, value = "operationId") final String operationId,
            @JsonProperty("summary") final String summary,
            @JsonProperty("description") final String description,
            @JsonProperty("tags") final List<Tag> tags,
            @JsonProperty("bindings") final Bindings bindings,
            @JsonProperty("traits") final Map traits,
            @JsonProperty("message") final Message message) {
        this.operationId = requireNonNull(operationId, "operationId");
        this.summary = summary;
        this.description = description;
        this.tags = tags == null ? List.of() : List.copyOf(tags);
        this.bindings = bindings;
        this.traits = traits;
        this.message = message;
    }

    /**
     * @return schema info
     */
    public Optional<SchemaInfo> schemaInfo() {
        if (message == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(message.schemas());
        } catch (Exception e) {
            throw new APIParserException(
                    "Error extracting schemas from (publish|subscribe) operation: " + operationId,
                    e);
        }
    }
}
