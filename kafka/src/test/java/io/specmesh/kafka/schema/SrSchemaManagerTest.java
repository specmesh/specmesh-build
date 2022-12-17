package io.specmesh.kafka.schema;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.junit.jupiter.api.Test;
import simple.schema_demo._public.user_checkout_value.UserCheckout;
import simple.schema_demo._public.user_signed_up_value.UserSignedUp;

class SrSchemaManagerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient(
            List.of(new AvroSchemaProvider(), new JsonSchemaProvider()));
    private final SrSchemaManager srSchemaManager = new SrSchemaManager(schemaRegistryClient);

    @Test
    public void shouldLoadJsonSchemaThenSerializeAndDeseralize() throws Exception {

        final var topicSubject = "simple.schema_demo._public.user_checkout";
        final var schemaContent = JsonSchemas.yamlToJson(Files.readString(
                Path.of(Objects.requireNonNull(getClass().getResource("/schema/" + topicSubject + ".yml")).toURI()),
                UTF_8));

        final var jsonSchema = new JsonSchema(schemaContent);

        jsonSchema.validate();

        final var schemaId = schemaRegistryClient.register(topicSubject + "-value", jsonSchema);

        assertThat(schemaId, is(1));

        final var userCheckout = new UserCheckout(100L, "joe bloggs", 100, "now");

        final Map<String, ?> props = Map.of(KafkaJsonSchemaSerializerConfig.AUTO_REGISTER_SCHEMAS, "false",
                // schema-reflect MUST be true when writing Java objects (otherwise you send a
                // datum-container instead of a Pogo)
                KafkaJsonSchemaSerializerConfig.SCHEMA_REFLECTION_CONFIG, "true",
                KafkaJsonSchemaSerializerConfig.USE_LATEST_VERSION, "true",
                KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://",
                KafkaJsonSchemaSerializerConfig.FAIL_INVALID_SCHEMA, true,
                KafkaJsonSchemaSerializerConfig.WRITE_DATES_AS_ISO8601, true,
                KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, UserCheckout.class.getName());
        final var serializer = new KafkaJsonSchemaSerializer(schemaRegistryClient, props);

        final byte[] bytess = serializer.serialize(topicSubject, userCheckout);

        final var deserializer = new KafkaJsonSchemaDeserializer(schemaRegistryClient, props);
        final var deserialize = (UserCheckout) deserializer.deserialize(topicSubject, bytess);

        assertThat(deserialize, is(userCheckout));
    }

    @Test
    public void shouldLoadAvroSchemaThenSerializeAndDeseralizeWithReflection() throws Exception {

        final var topicSubject = "simple.schema_demo._public.user_signed_up";
        final var schemaContent = Files.readString(
                Path.of(Objects.requireNonNull(getClass().getResource("/schema/" + topicSubject + ".avsc")).toURI()),
                UTF_8);

        final var avroSchema = new AvroSchema(schemaContent);

        final var schemaId = schemaRegistryClient.register(topicSubject + "-value", avroSchema);

        assertThat(schemaId, is(1));

        final UserSignedUp signedUp = new UserSignedUp("joe bloggs", "bloggy@wasmail.com", 100);

        final Map<String, ?> props = Map.of(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, "false",
                // schema-reflect MUST be true when writing Java objects (otherwise you send a
                // datum-container instead of a Pogo)
                KafkaAvroSerializerConfig.SCHEMA_REFLECTION_CONFIG, "true",
                KafkaAvroSerializerConfig.USE_LATEST_VERSION, "true",
                KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://");
        final KafkaAvroSerializer serializer = new KafkaAvroSerializer(schemaRegistryClient, props);

        final byte[] bytess = serializer.serialize(topicSubject, signedUp);

        final KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(schemaRegistryClient, props);
        final UserSignedUp deserialize = (UserSignedUp) deserializer.deserialize(topicSubject, bytess);

        assertThat(deserialize, is(signedUp));
    }

    @Test
    public void shouldLoadSchemaFromClasspath() throws Exception {
        // Given:
        final String subject = "topic.a.value";
        final Path schemaPath = Paths.get("test_party_schema.yml");
        final JsonSchema expectedSchema = loadSchemaFromClasspath(schemaPath.toString());
        final int expectedId = schemaRegistryClient.register(subject, expectedSchema);

        // When:
        final RegisteredSchema schema = srSchemaManager.loadFromClasspath(schemaPath, subject);

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
        final RegisteredSchema result = srSchemaManager.loadLatest(subject);

        // Then:
        assertThat(result, is(new RegisteredSchema(subject, latestSchema, latestId)));
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
        final RegisteredSchema actualV1 = srSchemaManager.loadById(subject, v1Id);
        final RegisteredSchema actualV2 = srSchemaManager.loadById(subject, v2Id);

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
        final RegisteredSchema result = srSchemaManager.registerFromClasspath(schemaFile, subject);

        // Then:
        final JsonSchema expectedSchema = loadSchemaFromClasspath(schemaFile.toString());
        final int expectedId = schemaRegistryClient.getId(result.subject(), expectedSchema);
        assertThat(result, is(new RegisteredSchema(subject, expectedSchema, expectedId)));
    }

    @Test
    public void shouldRegisterNewVersionOfSchema() throws Exception {
        // Given:
        final String subject = "some.topic.value";
        final Path schemaFile = Paths.get("test_party_v2_schema.yml");
        schemaRegistryClient.register(subject, loadSchemaFromClasspath("test_party_schema.yml"));

        // When:
        final RegisteredSchema result = srSchemaManager.registerFromClasspath(schemaFile, subject);

        // Then:
        final JsonSchema expectedSchema = loadSchemaFromClasspath(schemaFile.toString());
        final int expectedId = schemaRegistryClient.getId(result.subject(), expectedSchema);
        assertThat(result, is(new RegisteredSchema(subject, expectedSchema, expectedId)));
    }

    public static JsonSchema loadSchemaFromClasspath(final String schemaFile) {
        try {
            return new JsonSchema(yamlToJson(Files.readString(Path
                    .of(Objects.requireNonNull(RegisteredSchema.class.getResource("/schema/" + schemaFile)).toURI()),
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
