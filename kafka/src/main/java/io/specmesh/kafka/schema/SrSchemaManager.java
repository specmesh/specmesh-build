package io.specmesh.kafka.schema;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;

import java.nio.file.Path;

import static java.util.Objects.requireNonNull;
public class SrSchemaManager {
    private final SchemaRegistryClient schemaRegistry;

    public SrSchemaManager(final SchemaRegistryClient schemaRegistry) {
        this.schemaRegistry = requireNonNull(schemaRegistry, "schemaRegistry");
    }

    public <T> RegisteredSchema<T> loadFromClasspath(
            final Path schemaFile, final String subject) {
        final JsonSchema jsonSchema = JsonSchemas.loadFromClasspath(schemaFile);

        try {
            final int id = schemaRegistry.getId(subject, jsonSchema);
            return new RegisteredSchema<>(subject, jsonSchema, id);
        } catch (final Exception e) {
            throw new RuntimeException(subject + ":" + schemaFile, e);
        }
    }

    public <T> RegisteredSchema<T> loadLatest(final String subject) {
        try {
            final SchemaMetadata latestSchemaMetadata =
                    schemaRegistry.getLatestSchemaMetadata(subject);
            final int id = latestSchemaMetadata.getId();
            return new RegisteredSchema<>(
                    subject, new JsonSchema(latestSchemaMetadata.getSchema()), id);
        } catch (final Exception e) {
            throw new RuntimeException(subject, e);
        }
    }

    public RegisteredSchema loadById(
            final String subject, final int schemaId) {
        try {
            return new RegisteredSchema<>(
                    subject, schemaRegistry.getSchemaById(schemaId), schemaId);
        } catch (final Exception e) {
            throw new RuntimeException(subject + ":" + schemaId, e);
        }
    }

    public <T> RegisteredSchema<T> registerFromClasspath(
            final Path schemaFile, final String subject) {
        final JsonSchema schema = JsonSchemas.loadFromClasspath(schemaFile);

        try {
            final int schemaId = schemaRegistry.register(subject, schema);
            return new RegisteredSchema<>(subject, schema, schemaId);
        } catch (final Exception e) {
            throw new RuntimeException(subject + ":" + schema, e);
        }
    }
}
