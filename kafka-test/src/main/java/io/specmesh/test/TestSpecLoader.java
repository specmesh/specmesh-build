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

package io.specmesh.test;


import io.specmesh.apiparser.AsyncApiParser;
import io.specmesh.kafka.KafkaApiSpec;
import java.io.IOException;

/** Util class for use in tests to load ApiSpecs. */
public final class TestSpecLoader {

    private TestSpecLoader() {}

    /**
     * Load a spec from the classpath.
     *
     * @param spec the spec to load.
     * @return returns the loaded spec.
     */
    public static KafkaApiSpec loadFromClassPath(final String spec) {
        return loadFromClassPath(spec, TestSpecLoader.class.getClassLoader());
    }

    /**
     * Load a spec from the classpath.
     *
     * @param spec the spec to load.
     * @param classLoader the class loader to use.
     * @return returns the loaded spec.
     */
    public static KafkaApiSpec loadFromClassPath(final String spec, final ClassLoader classLoader) {
        try {
            return new KafkaApiSpec(
                    new AsyncApiParser().loadResource(classLoader.getResourceAsStream(spec)));
        } catch (IOException e) {
            throw new RuntimeException("Failed to load api spec", e);
        }
    }
}
