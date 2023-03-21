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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/** Pojo representing the api spec */
@Builder
@Data
@Accessors(fluent = true)
@JsonIgnoreProperties(ignoreUnknown = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@SuppressFBWarnings
public class ApiSpec {
    private static final char DELIMITER = System.getProperty("DELIMITER", ".").charAt(0);
    @JsonProperty private String id;

    @JsonProperty private String version;

    @JsonProperty private String asyncapi;

    @JsonProperty private Map<String, Channel> channels;

    /**
     * @return unique spec id
     */
    public String id() {
        return validate(id).substring("urn:".length()).replace(":", DELIMITER + "");
    }

    private String validate(final String id) {
        if (!id.startsWith("urn:")) {
            throw new IllegalStateException("ID must be formatted as a URN, expecting urn:");
        }
        return id;
    }

    /**
     * @return channels
     */
    public Map<String, Channel> channels() {
        return channels.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                e -> getCanonical(id(), e.getKey()),
                                Map.Entry::getValue,
                                (k, v) -> k,
                                LinkedHashMap::new));
    }

    private String getCanonical(final String id, final String channelName) {
        if (channelName.startsWith("/")) {
            return channelName.substring(1).replace('/', DELIMITER);
        } else {
            return id + DELIMITER + channelName.replace('/', DELIMITER);
        }
    }
}
