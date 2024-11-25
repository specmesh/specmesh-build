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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import java.util.Map;
import org.junit.jupiter.api.Test;

class ApiSpecTest {

    @Test
    void shouldThrowOnEmptyDomainId() {
        // When:
        final Exception e =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> new ApiSpec("urn: ", "1", "1", Map.of()));

        // Then:
        assertThat(
                e.getMessage(),
                is("ID must define a domain id after the urn, e.g. urn:some.domain"));
    }
}
