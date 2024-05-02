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

import lombok.Value;
import lombok.experimental.Accessors;

/** Pojo representing a schmea */
@Value
@Accessors(fluent = true)
public class SchemaInfo {
    private final String schemaRef;
    private final String schemaFormat;
    private final String schemaIdLocation;
    private final String contentType;
    private String schemaLookupStrategy;

    /**
     * @param schemaRef location of schema
     * @param schemaFormat format of schema
     * @param schemaIdLocation header || payload
     * @param contentType content type of schema
     * @param schemaLookupStrategy schema lookup strategy
     */
    public SchemaInfo(
            final String schemaRef,
            final String schemaFormat,
            final String schemaIdLocation,
            final String contentType,
            final String schemaLookupStrategy) {
        this.schemaRef = schemaRef;
        this.schemaFormat = requireNonNull(schemaFormat, "schemaFormat");
        this.schemaIdLocation = requireNonNull(schemaIdLocation, "schemaIdLocation");
        this.contentType = requireNonNull(contentType, "contentType");
        this.schemaLookupStrategy = requireNonNull(schemaLookupStrategy, "schemaLookupStrategy");
    }
}
