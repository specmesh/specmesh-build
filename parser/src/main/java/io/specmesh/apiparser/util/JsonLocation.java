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

package io.specmesh.apiparser.util;

import com.fasterxml.jackson.core.JsonParser;
import java.io.File;
import java.net.URI;

/**
 * Util class for determining the current location within a JSON/YAML file being parsed.
 *
 * <p>Locations are in the URI form: {@code file:///path/to/file.yml:<line number>}.
 */
public final class JsonLocation {

    private static final URI UNKNOWN = URI.create("unknown");

    private JsonLocation() {}

    /**
     * Get the current location from the parser.
     *
     * @param parser the parser.
     * @return the current location.
     */
    public static URI location(final JsonParser parser) {
        return location(parser.currentLocation());
    }

    /**
     * Get the location from the Jackson location
     *
     * @param location the Jackson location
     * @return the location.
     */
    public static URI location(final com.fasterxml.jackson.core.JsonLocation location) {
        final Object content = location.contentReference().getRawContent();
        if (!(content instanceof File)) {
            return UNKNOWN;
        }

        final String filePath =
                ((File) content).toURI().toString().replaceFirst("file:/", "file:///");

        return URI.create(filePath + ":" + location.getLineNr());
    }
}
