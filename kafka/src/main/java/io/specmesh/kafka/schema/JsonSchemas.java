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

package io.specmesh.kafka.schema;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.google.common.annotations.VisibleForTesting;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.io.IOUtils;

/**
 * Util class for working with Json schemas.
 */
public final class JsonSchemas {

    private static final Path SCHEMA_DIRECTORY = Paths.get("schema");

    private static final ObjectMapper yamlMapper = new ObjectMapper(
            new YAMLFactory().enable(YAMLGenerator.Feature.MINIMIZE_QUOTES));
    private static final ObjectMapper jsonWriter = new ObjectMapper();

    private JsonSchemas() {
    }

    /**
     * Load a schema from the classpath.
     *
     * @param schemaFile
     *            the path to the schema.
     * @return the schema.
     */
    public static JsonSchema loadFromClasspath(final String schemaFile) {
        return loadFromClasspath(Paths.get(schemaFile));
    }

    /**
     * Load a schema from the classpath.
     *
     * @param schemaFile
     *            the path to the schema.
     * @return the schema.
     */
    public static JsonSchema loadFromClasspath(final Path schemaFile) {
        final String path = File.separator + SCHEMA_DIRECTORY.resolve(schemaFile);
        final URL resource = JsonSchemas.class.getResource(path);
        if (resource == null) {
            throw new RuntimeException("Failed to load schema resource: " + path + ". Resource not found");
        }

        try {
            return new JsonSchema(yamlToJson(loadYamlFromUrl(resource)));
        } catch (final Exception e) {
            throw new RuntimeException("Failed to convert schema resource: " + path + ". " + e.getMessage(), e);
        }
    }

    @VisibleForTesting
    static String loadYamlFromUrl(final URL resource) throws IOException {
        return new String(IOUtils.toByteArray(resource), StandardCharsets.UTF_8);
    }

    /**
     * Convert a YAML into JSON.
     *
     * @param yaml
     *            the YAML to convert
     * @return the JSON
     * @throws IOException
     *             on invalid YAML
     */
    public static String yamlToJson(final String yaml) throws IOException {
        final Object obj = yamlMapper.readValue(yaml, Object.class);
        return jsonWriter.writeValueAsString(obj);
    }
}
