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
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
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
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Accessors(fluent = true)
@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressFBWarnings
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@SuppressWarnings("rawtypes")
public class Operation {
    @EqualsAndHashCode.Include @JsonProperty private String operationId;

    @JsonProperty private String summary;

    @JsonProperty private String description;

    @SuppressWarnings("rawtypes")
    @JsonProperty
    @Builder.Default
    private List<Tag> tags = List.of();

    @JsonProperty private Bindings bindings;

    // https://www.asyncapi.com/docs/reference/specification/v2.4.0#operationTraitObject
    @JsonProperty private Map traits;

    @JsonProperty private Message message;

    /**
     * @return schema info
     */
    public SchemaInfo schemaInfo() {
        if (message.bindings() == null) {
            throw new APIParserException(
                    "Bindings not found for (publish|subscribe) operation: " + operationId);
        }
        return new SchemaInfo(
                message().schemaRef(),
                message().schemaFormat(),
                message.bindings().kafka().schemaIdLocation(),
                message().contentType(),
                message.bindings().kafka().schemaLookupStrategy());
    }

    public void validate() {
        if (operationId == null) {
            throw new APIParserException("(publish|subscribe) operationId  is null");
        }
    }

    public boolean isSchemaRequired() {
        return this.message() != null
                && this.message().schemaRef() != null
                && this.message().schemaRef().length() > 0;
    }
}
