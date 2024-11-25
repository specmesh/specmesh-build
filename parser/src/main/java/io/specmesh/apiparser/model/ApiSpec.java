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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.Accessors;

/** Pojo representing the api spec */
@Builder
@Data
@Accessors(fluent = true)
@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressFBWarnings
public final class ApiSpec {
    public static final char DELIMITER = System.getProperty("DELIMITER", ".").charAt(0);
    public static final String SPECMESH_PUBLIC = "specmesh.public";

    public static final String PUBLIC = System.getProperty(SPECMESH_PUBLIC, "_public");

    public static final String SPECMESH_PROTECTED = "specmesh.protected";
    public static final String PROTECTED = System.getProperty(SPECMESH_PROTECTED, "_protected");
    public static final String SPECMESH_PRIVATE = "specmesh.private";
    public static final String PRIVATE = System.getProperty(SPECMESH_PRIVATE, "_private");
    public static final String URN_PREFIX = "urn:";

    @JsonProperty private String id;

    @JsonProperty private String version;

    @JsonProperty private String asyncapi;

    @JsonProperty private Map<String, Channel> channels;

    @JsonCreator
    ApiSpec(
            @JsonProperty("id") final String id,
            @JsonProperty("version") final String version,
            @JsonProperty("asyncapi") final String asyncapi,
            @JsonProperty("channels") final Map<String, Channel> channels) {
        this.id = requireNonNull(id, "id");
        this.version = version;
        this.asyncapi = asyncapi;
        this.channels = channels;

        if (!id.startsWith(URN_PREFIX)) {
            throw new IllegalArgumentException("ID must be formatted as a URN, expecting urn:");
        }

        if (id.substring(URN_PREFIX.length()).isBlank()) {
            throw new IllegalArgumentException(
                    "ID must define a domain id after the urn, e.g. urn:some.domain");
        }
    }

    /**
     * @return unique spec id
     */
    public String id() {
        return id.substring(URN_PREFIX.length()).replace(":", DELIMITER + "");
    }

    /**
     * @return channels with names formatted using canonical path
     */
    public Map<String, Channel> channels() {
        return channels.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                e -> getCanonical(id(), e.getKey(), e.getValue().publish() != null),
                                Map.Entry::getValue,
                                (k, v) -> k,
                                LinkedHashMap::new));
    }

    private String getCanonical(final String id, final String channelName, final boolean publish) {
        // legacy and deprecated
        if (channelName.startsWith("/")) {
            return channelName.substring(1).replace('/', DELIMITER);
        }
        // should not occur unless subdomains are being used
        if (channelName.startsWith(id)) {
            return channelName;
        }
        if (publish) {
            return id + DELIMITER + channelName.replace('/', DELIMITER);
        } else {
            return channelName;
        }
    }
}
