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

import io.specmesh.apiparser.model.ApiSpec;

/** Used to export an APISpec to structured text */
public interface ExporterWriter {
    /**
     * export to structured text
     *
     * @param apiSpec - to export
     * @return - resultant structured text
     * @throws Exporter.ExporterException - when failing to convert
     */
    String export(ApiSpec apiSpec) throws Exporter.ExporterException;
}
