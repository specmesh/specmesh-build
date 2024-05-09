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

package io.specmesh.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.specmesh.apiparser.model.ApiSpec;
import io.specmesh.apiparser.parse.SpecMapper;

/** Export the Spec object to its yaml representation */
public class ExporterYamlWriter implements ExporterWriter {

    /**
     * Export the Spec object to its yaml representation
     *
     * @param apiSpec - the hydrated spec to convert to yaml
     * @return the asyncapi spec as yaml
     * @throws Exporter.ExporterException - when json cannot be handled
     */
    @Override
    public String export(final ApiSpec apiSpec) throws Exporter.ExporterException {
        try {
            return SpecMapper.mapper().writeValueAsString(apiSpec);
        } catch (JsonProcessingException e) {
            throw new Exporter.ExporterException("Failed to convert to YAML", e);
        }
    }
}
