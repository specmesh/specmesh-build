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

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.HashMap;
import java.util.Map;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Accessors(fluent = true)
@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
@SuppressFBWarnings
public class PayloadProperties {

    @JsonProperty private KeyValue key;

    @JsonProperty private KeyValue value;

    @JsonAnyGetter private Map<String, Object> otherProperties = new HashMap<>();

    @JsonAnySetter
    public void set(final String name, final Object value) {
        otherProperties.put(name, value);
    }

    public boolean isKeyValue() {
        return this.key != null && this.value != null;
    }

    public boolean isObject() {
        return !this.otherProperties.isEmpty();
    }

    public static class KeyValue {

        @JsonProperty("$ref")
        private String ref;

        @JsonProperty() private String type;

        public String type() {
            return type;
        }

        public String ref() {
            return ref;
        }
    }
}
