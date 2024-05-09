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

package io.specmesh.apiparser;

import io.specmesh.apiparser.model.ApiSpec;
import io.specmesh.apiparser.parse.SpecMapper;
import java.io.IOException;
import java.io.InputStream;

/** The parser */
public class AsyncApiParser {

    /**
     * Parse an {@link ApiSpec} from the supplied {@code inputStream}.
     *
     * @param inputStream stream containing the spec.
     * @return the api spec
     * @throws IOException on error
     */
    public final ApiSpec loadResource(final InputStream inputStream) throws IOException {
        if (inputStream == null || inputStream.available() == 0) {
            throw new RuntimeException("Not found");
        }
        return SpecMapper.mapper().readValue(inputStream, ApiSpec.class);
    }

    public static class APIParserException extends RuntimeException {
        public APIParserException(final String message, final Exception cause) {
            super(message, cause);
        }

        public APIParserException(final String message) {
            super(message);
        }
    }
}
