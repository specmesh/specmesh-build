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
import io.specmesh.apiparser.AsyncApiParser;
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
    public static final char DELIMITER = System.getProperty("DELIMITER", ".").charAt(0);
    public static final String SPECMESH_PUBLIC = "specmesh.public";

    public static final String PUBLIC = System.getProperty(SPECMESH_PUBLIC, "_public");

    public static final String SPECMESH_PROTECTED = "specmesh.protected";
    public static final String PROTECTED = System.getProperty(SPECMESH_PROTECTED, "_protected");
    public static final String SPECMESH_PRIVATE = "specmesh.private";
    public static final String PRIVATE = System.getProperty(SPECMESH_PRIVATE, "_private");

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

    public void validate() {
        this.validate(this.id);
        this.channels()
                .forEach(
                        (key, value) -> {
                            try {
                                value.validate();
                            } catch (Exception ex) {
                                throw new AsyncApiParser.APIParserException(
                                        "Validate failed for:" + key, ex);
                            }
                        });
    }
}
