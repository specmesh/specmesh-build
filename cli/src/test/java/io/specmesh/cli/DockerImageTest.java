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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

@Tag("ContainerisedTest")
class DockerImageTest {

    private GenericContainer<?> container;

    @BeforeEach
    void setUp() {
        container = new GenericContainer<>("ghcr.io/specmesh/specmesh-build-cli:latest");
    }

    @AfterEach
    void tearDown() {
        if (container != null) {
            container.stop();
        }
    }

    @Test
    void shouldShowUsageWhenRunWithoutArgs() {
        // When:
        try {
            container.start();
        } catch (Exception e) {
            // Expected to fail as CLI tools exit immediately
        }

        // Then:
        final String logs = container.getLogs();
        assertThat(logs, containsString("Usage"));
        assertThat(logs, containsString("Commands"));
        assertThat(logs, containsString("provision"));
        assertThat(logs, containsString("--bootstrap-server"));
    }
}
