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

public final class JsonSchemas {

    public static final Path SCHEMA_DIRECTORY = Paths.get("schema");

    private static final ObjectMapper yamlMapper =
            new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.MINIMIZE_QUOTES));
    private static final ObjectMapper jsonWriter = new ObjectMapper();

    private JsonSchemas() {}

    public static JsonSchema loadFromClasspath(final String schemaFile) {
        return loadFromClasspath(Paths.get(schemaFile));
    }

    public static JsonSchema loadFromClasspath(final Path schemaFile) {
        final String path = File.separator + SCHEMA_DIRECTORY.resolve(schemaFile);
        final URL resource = JsonSchemas.class.getResource(path);
        if (resource == null) {
            throw new RuntimeException(
                    "Failed to load schema resource: " + path + ". Resource not found");
        }

        try {
            return new JsonSchema(yamlToJson(loadYamlFromUrl(resource)));
        } catch (final Exception e) {
            throw new RuntimeException(
                    "Failed to convert schema resource: " + path + ". " + e.getMessage(), e);
        }
    }

    @VisibleForTesting
    static String loadYamlFromUrl(final URL resource) throws IOException {
        return new String(IOUtils.toByteArray(resource), StandardCharsets.UTF_8);
    }

    public static String yamlToJson(final String yaml) throws IOException {
        final Object obj = yamlMapper.readValue(yaml, Object.class);
        return jsonWriter.writeValueAsString(obj);
    }
}
