package io.specmesh.kafka.schema;

import static java.util.Objects.requireNonNull;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import java.nio.file.Path;

/**
 * Wrapper around the {@link SchemaRegistryClient}
 */
public class SrSchemaManager {
    private final SchemaRegistryClient schemaRegistry;

    /**
     * SR client wrapper
     *
     * @param schemaRegistry
     *            the client
     */
    public SrSchemaManager(final SchemaRegistryClient schemaRegistry) {
        this.schemaRegistry = requireNonNull(schemaRegistry, "schemaRegistry");
    }

    /**
     * Load a schema from the classpath into the local schema cache.
     *
     * @param schemaFile
     *            the path to the schema file
     * @param subject
     *            the subject under which to store it
     * @return the registered schema
     */
    public RegisteredSchema loadFromClasspath(final Path schemaFile, final String subject) {
        final JsonSchema jsonSchema = JsonSchemas.loadFromClasspath(schemaFile);

        try {
            final int id = schemaRegistry.getId(subject, jsonSchema);
            return new RegisteredSchema(subject, jsonSchema, id);
        } catch (final Exception e) {
            throw new RuntimeException(subject + ":" + schemaFile, e);
        }
    }

    /**
     * Load the latest version of a schema into the local schema cache.
     *
     * @param subject
     *            the schema subject to load.
     * @return the registered schema
     */
    public RegisteredSchema loadLatest(final String subject) {
        try {
            final SchemaMetadata latestSchemaMetadata = schemaRegistry.getLatestSchemaMetadata(subject);
            final int id = latestSchemaMetadata.getId();
            return new RegisteredSchema(subject, new JsonSchema(latestSchemaMetadata.getSchema()), id);
        } catch (final Exception e) {
            throw new RuntimeException(subject, e);
        }
    }

    /**
     * Load a specific version of a schema into the local schema cache.
     *
     * @param subject
     *            the schema subject
     * @param schemaId
     *            the unique schema id.
     * @return the registered schema.
     */
    public RegisteredSchema loadById(final String subject, final int schemaId) {
        try {
            return new RegisteredSchema(subject, schemaRegistry.getSchemaById(schemaId), schemaId);
        } catch (final Exception e) {
            throw new RuntimeException(subject + ":" + schemaId, e);
        }
    }

    /**
     * Register a schema with the schema registry from a file in the classpath.
     *
     * @param schemaFile
     *            the path to the schema file
     * @param subject
     *            the subject under which to register it
     * @return the registered schema.
     */
    public RegisteredSchema registerFromClasspath(final Path schemaFile, final String subject) {
        final JsonSchema schema = JsonSchemas.loadFromClasspath(schemaFile);

        try {
            final int schemaId = schemaRegistry.register(subject, schema);
            return new RegisteredSchema(subject, schema, schemaId);
        } catch (final Exception e) {
            throw new RuntimeException(subject + ":" + schema, e);
        }
    }
}
