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

package io.specmesh.cli;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

class FlattenTest {

    /**
     * Generate flattened/prefixed version
     *
     * @throws Exception - when broken
     */
    @Test
    void shouldFlattenIt() throws Exception {

        final var flatten = Flatten.builder().build();

        final var outSpexc = "build/finished-spec.yml";

        // Given:
        final CommandLine.ParseResult parseResult =
                new CommandLine(flatten)
                        .parseArgs(
                                ("--in-spec simple_schema_demo-api.yaml"
                                                + " --out-spec "
                                                + outSpexc)
                                        .split(" "));

        assertThat(parseResult.matchedArgs().size(), is(2));

        assertThat(flatten.call(), is(0));

        assertThat(new File(outSpexc).exists(), is(true));

        try (FileInputStream fis = new FileInputStream(outSpexc)) {
            final var testOutput = new String(fis.readAllBytes(), StandardCharsets.UTF_8);
            assertThat(testOutput, containsString("simple.schema_demo._public.user_signed_up:"));
        }
    }
}
