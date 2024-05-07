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

public final class TopicSchema {

    public enum Part {
        key,
        value
    };

    private final Part part;
    private final String schemaRef;
    private final String lookupStrategy;

    public TopicSchema(final Part part, final String schemaRef, final String lookupStrategy) {
        this.part = requireNonNull(part, "part");
        this.schemaRef = requireNonNull(schemaRef, "schemaRef");
        this.lookupStrategy = requireNonNull(lookupStrategy, "lookupStrategy");
    }

    public Part part() {
        return part;
    }

    public String schemaRef() {
        return schemaRef;
    }

    public String schemaLookupStrategy() {
        return lookupStrategy;
    }
}
