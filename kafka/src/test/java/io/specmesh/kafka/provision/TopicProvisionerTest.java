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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

import java.util.List;
import org.junit.jupiter.api.Test;

class TopicProvisionerTest {

    @Test
    void shouldNotSwallowExceptions() {
        final var topic = TopicProvisioner.Topic.builder().build();
        final var swallowed =
                Status.builder()
                        .topics(
                                List.of(
                                        topic.exception(
                                                new RuntimeException(
                                                        new RuntimeException("swallowed")))))
                        .build();
        assertThat(
                swallowed.topics().iterator().next().exception().toString(),
                containsString(".java:"));
    }
}
