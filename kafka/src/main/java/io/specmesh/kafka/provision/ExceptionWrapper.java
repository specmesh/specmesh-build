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

package io.specmesh.kafka.provision;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.PrintWriter;
import java.io.StringWriter;

@SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "exception copy")
public class ExceptionWrapper extends RuntimeException {

    public ExceptionWrapper(final Exception exception) {
        super(exception.getMessage(), exception);
    }

    @Override
    public String toString() {
        final var sb = new StringBuilder();
        sb.append(getClass().getName()).append(": ").append(getMessage()).append("\n");

        final var sw = new StringWriter();
        final var pw = new PrintWriter(sw);
        getCause().printStackTrace(pw);
        sb.append(sw);
        return sb.toString();
    }
}
