package io.specmesh.kafka.schema;


import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;

class SrSchemaManagerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    private final SrSchemaManager srSchemaManager = new SrSchemaManager(schemaRegistryClient);

    @Test
    public void shouldLoadSchemaFromClasspath() throws Exception {
        // Given:
        final String subject = "topic.a.value";
        final Path schemaPath = Paths.get("test_party_schema.yml");
        final JsonSchema expectedSchema = loadSchemaFromClasspath(schemaPath.toString());
        final int expectedId = schemaRegistryClient.register(subject, expectedSchema);

        // When:
        final RegisteredSchema schema =
                srSchemaManager.loadFromClasspath(schemaPath, subject);

        // Then:
        assertThat(schema.id(), is(expectedId));
        assertThat(schema.schema(), is(expectedSchema));
        assertThat(schema.subject(), is(subject));
    }

    @Test
    public void shouldLoadLatestSchema() throws Exception {
        // Given:
        final String subject = "some.topic.value";
        schemaRegistryClient.register(subject, loadSchemaFromClasspath("test_model_v2_schema.yml"));

        final JsonSchema latestSchema = loadSchemaFromClasspath("test_model_v2_schema.yml");
        final int latestId = schemaRegistryClient.register(subject, latestSchema);

        // When:
        final RegisteredSchema result =
                srSchemaManager.loadLatest(subject);

        // Then:
        assertThat(
                result,
                is(new RegisteredSchema(subject, latestSchema, latestId)));
    }


    @Test
    public void shouldLoadByIdFromSchemaRegistry() throws Exception {
        // Given:
        final String subject = "some.topic.value";
        final JsonSchema v1Schema = loadSchemaFromClasspath("test_party_schema.yml");
        final int v1Id = schemaRegistryClient.register(subject, v1Schema);
        final JsonSchema v2Schema = loadSchemaFromClasspath("test_party_v2_schema.yml");
        final int v2Id = schemaRegistryClient.register(subject, v2Schema);

        // When:
        final RegisteredSchema actualV1 =
                srSchemaManager.loadById(subject, v1Id);
        final RegisteredSchema actualV2 =
                srSchemaManager.loadById(subject, v2Id);

        // Then:
        assertThat(actualV1, is(new RegisteredSchema(subject, v1Schema, v1Id)));
        assertThat(actualV2, is(new RegisteredSchema(subject, v2Schema, v2Id)));
    }



    @Test
    public void shouldRegisterSchema() throws Exception {
        // Given:
        final String subject = "some.topic.value";
        final Path schemaFile = Paths.get("test_party_schema.yml");

        // When:
        final RegisteredSchema result =
                srSchemaManager.registerFromClasspath(schemaFile, subject);

        // Then:
        final JsonSchema expectedSchema = loadSchemaFromClasspath(schemaFile.toString());
        final int expectedId = schemaRegistryClient.getId(result.subject(), expectedSchema);
        assertThat(
                result,
                is(new RegisteredSchema(subject, expectedSchema, expectedId)));
    }

    @Test
    public void shouldRegisterNewVersionOfSchema() throws Exception {
        // Given:
        final String subject = "some.topic.value";
        final Path schemaFile = Paths.get("test_party_v2_schema.yml");
        schemaRegistryClient.register(subject, loadSchemaFromClasspath("test_party_schema.yml"));

        // When:
        final RegisteredSchema result =
                srSchemaManager.registerFromClasspath(schemaFile, subject);

        // Then:
        final JsonSchema expectedSchema = loadSchemaFromClasspath(schemaFile.toString());
        final int expectedId = schemaRegistryClient.getId(result.subject(), expectedSchema);
        assertThat(
                result,
                is(new RegisteredSchema(subject, expectedSchema, expectedId)));
    }

    public static JsonSchema loadSchemaFromClasspath(final String schemaFile) {
        try {
            return new JsonSchema(
                    yamlToJson(
                            Files.readString(
                                    Path.of(
                                            RegisteredSchema.class
                                                    .getResource("/schema/" + schemaFile)
                                                    .toURI()),
                                    UTF_8)));
        } catch (final Exception e) {
            throw new AssertionError("Failed to load schemaFile: " + schemaFile, e);
        }
    }
    public static String yamlToJson(final String yaml) throws IOException {
        final ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
        final Object obj = yamlReader.readValue(yaml, Object.class);
        return MAPPER.writeValueAsString(obj);
    }
}
