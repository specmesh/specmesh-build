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

import java.util.Optional;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;

/** Pojo representing a schmea */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@Builder
@Value
@Accessors(fluent = true)
public class SchemaInfo {
    private final Optional<RecordPart> key;
    private final RecordPart value;
    private final Optional<String> schemaFormat;
    private final Optional<String> schemaIdLocation;
    private final Optional<String> contentType;
    private final Optional<String> schemaLookupStrategy;

    /**
     * @param key schema info for the key of the Kafka Record
     * @param value schema info for the value of the Kafka Record
     * @param schemaFormat format of schema
     * @param schemaIdLocation header || payload
     * @param contentType content type of schema
     * @param schemaLookupStrategy schema lookup strategy
     */
    public SchemaInfo(
            final Optional<RecordPart> key,
            final RecordPart value,
            final Optional<String> schemaFormat,
            final Optional<String> schemaIdLocation,
            final Optional<String> contentType,
            final Optional<String> schemaLookupStrategy) {
        this.key = requireNonNull(key, "key");
        this.value = requireNonNull(value, "value");
        this.schemaFormat = requireNonNull(schemaFormat, "schemaFormat");
        this.schemaIdLocation = requireNonNull(schemaIdLocation, "schemaIdLocation");
        this.contentType = requireNonNull(contentType, "contentType");
        this.schemaLookupStrategy = requireNonNull(schemaLookupStrategy, "schemaLookupStrategy");
    }
}
