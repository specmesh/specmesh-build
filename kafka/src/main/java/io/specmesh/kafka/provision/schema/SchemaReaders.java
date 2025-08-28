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

package io.specmesh.kafka.provision.schema;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.specmesh.kafka.provision.Status;
import io.specmesh.kafka.provision.schema.AvroReferenceFinder.DetectedSchema;
import io.specmesh.kafka.provision.schema.SchemaProvisioner.Schema;
import io.specmesh.kafka.provision.schema.SchemaProvisioner.SchemaProvisioningException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.experimental.Accessors;

/** Readers for reading Schemas */
public final class SchemaReaders {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
    }

    /** defensive */
    private SchemaReaders() {}

    public static final class LocalSchemaReader {

        private final ClassPathSchemaReader cpReader = new ClassPathSchemaReader();
        private final FileSystemSchemaReader fsReader = new FileSystemSchemaReader();

        public List<NamedSchema> read(final Path schemaFile) {
            if (cpReader.has(schemaFile)) {
                return cpReader.readLocal(schemaFile);
            } else {
                return fsReader.readLocal(schemaFile);
            }
        }
    }

    private abstract static class BaseLocalSchemaReader {

        /**
         * @param filePath path to schema.
         * @return ordered list of schema, with schema dependencies earlier in the list. The schema
         *     loaded from {@code filePath} will have an empty subject
         */
        public List<NamedSchema> readLocal(final Path filePath) {
            final String schemaContent = readSchema(filePath);
            final String filename =
                    Optional.ofNullable(filePath.getFileName()).map(Objects::toString).orElse("");
            if (filename.endsWith(".avsc")) {
                return referenceFinder(filePath)
                        .findReferences(filePath.toString(), schemaContent)
                        .stream()
                        .map(s -> new NamedSchema(s.name(), toAvroSchema(s)))
                        .collect(Collectors.toList());
            } else if (filename.endsWith(".yml")) {
                return List.of(new NamedSchema("", new JsonSchema(schemaContent)));
            } else if (filename.endsWith(".proto")) {
                return List.of(new NamedSchema("", new ProtobufSchema(schemaContent)));
            } else {
                throw new UnsupportedOperationException(
                        "Unsupported schema file: " + filePath.toAbsolutePath().normalize());
            }
        }

        protected abstract String readSchema(Path path);

        abstract AvroReferenceFinder referenceFinder(Path parentSchema);

        private static AvroSchema toAvroSchema(final DetectedSchema schema) {

            final List<SchemaReference> references =
                    schema.references().stream()
                            .map(ref -> new SchemaReference(ref.name(), ref.name(), -1))
                            .collect(Collectors.toList());

            final Map<String, String> resolvedReferences =
                    schema.references().stream()
                            .collect(
                                    toMap(
                                            DetectedSchema::name,
                                            DetectedSchema::content,
                                            (s1, s2) -> s1,
                                            LinkedHashMap::new));

            return new AvroSchema(schema.content(), references, resolvedReferences, -1);
        }
    }

    public static final class ClassPathSchemaReader extends BaseLocalSchemaReader {

        public boolean has(final Path schemaFile) {
            return getClass().getClassLoader().getResource(schemaFile.toString()) != null;
        }

        @Override
        protected String readSchema(final Path path) {
            try (InputStream s = getClass().getClassLoader().getResourceAsStream(path.toString())) {
                if (s == null) {
                    throw new RuntimeException(path + " not found");
                }
                return new String(s.readAllBytes(), StandardCharsets.UTF_8);
            } catch (Exception e) {
                throw new SchemaProvisioningException(
                        "Failed to read schema from classpath:" + path, e);
            }
        }

        @Override
        AvroReferenceFinder referenceFinder(final Path parentSchema) {
            final Path schemaDir =
                    Optional.ofNullable(parentSchema.getParent()).orElse(Path.of(""));

            return new AvroReferenceFinder(
                    type -> {
                        final Path path = schemaDir.resolve(type + ".avsc");
                        final String content = readSchema(path);
                        return new AvroReferenceFinder.LoadedSchema(path.toString(), content);
                    });
        }
    }

    public static final class FileSystemSchemaReader extends BaseLocalSchemaReader {

        protected String readSchema(final Path path) {
            try {
                return Files.readString(path, StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new SchemaProvisioningException(
                        "Failed to read schema at path:" + path.toAbsolutePath().normalize(), e);
            }
        }

        @Override
        AvroReferenceFinder referenceFinder(final Path parentSchema) {
            final Path schemaDir = parentSchema.toAbsolutePath().getParent();

            return new AvroReferenceFinder(
                    type -> {
                        final Path path = schemaDir.resolve(type + ".avsc");
                        final String content = readSchema(path);
                        return new AvroReferenceFinder.LoadedSchema(path.toString(), content);
                    });
        }
    }

    /** Read Schemas from registry for given prefix */
    public static final class SrSchemaReader implements SchemaReader {

        private final SchemaRegistryClient client;

        /**
         * defensive
         *
         * @param client - cluster connection
         */
        private SrSchemaReader(final SchemaRegistryClient client) {
            this.client = client;
        }

        /**
         * Read set of schemas for subject
         *
         * @param prefix to filter against
         * @return found acls with status set to READ
         */
        @Override
        public Collection<Schema> read(final String prefix) {

            try {
                final var subjects = client.getAllSubjectsByPrefix(prefix);
                final var schemas =
                        subjects.stream()
                                .collect(
                                        toMap(
                                                subject -> subject,
                                                subject -> {
                                                    try {
                                                        return client.getSchemas(
                                                                subject, false, true);
                                                    } catch (IOException | RestClientException e) {
                                                        throw new SchemaProvisioningException(
                                                                "Failed to load schemas", e);
                                                    }
                                                }));

                return schemas.entrySet().stream()
                        .filter(entry -> !entry.getValue().isEmpty())
                        .map(
                                entry ->
                                        Schema.builder()
                                                .subject(entry.getKey())
                                                .type(entry.getValue().get(0).schemaType())
                                                .schema(entry.getValue().get(0))
                                                .state(Status.STATE.READ)
                                                .build())
                        .collect(Collectors.toList());
            } catch (RestClientException | IOException e) {
                throw new SchemaProvisioningException("Failed to read schemas for:" + prefix, e);
            }
        }
    }

    /** Read Schema API */
    public interface SchemaReader {
        /**
         * read all schema with the supplied subject name {@code prefix}.
         *
         * @param prefix the subject name prefix
         * @return the found schema.
         */
        Collection<Schema> read(String prefix);
    }

    /**
     * berevity
     *
     * @return builder
     */
    public static SchemaReaderBuilder builder() {
        return SchemaReaderBuilder.builder();
    }

    /** builder */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "schema refs")
    public static final class SchemaReaderBuilder {

        private SchemaRegistryClient srClient;

        /** defensive */
        private SchemaReaderBuilder() {}

        /**
         * main builder
         *
         * @return builder
         */
        public static SchemaReaderBuilder builder() {
            return new SchemaReaderBuilder();
        }

        public SchemaReaderBuilder schemaRegistryClient(
                final SchemaRegistryClient schemaRegistryClient) {
            this.srClient = schemaRegistryClient;
            return this;
        }

        /**
         * build it
         *
         * @return the specified reader impl
         */
        public SchemaReader build() {
            return new SrSchemaReader(srClient);
        }
    }

    @SuppressFBWarnings(
            value = "EI_EXPOSE_REP",
            justification = "refs passed as param to prevent API pollution")
    @Data
    @Accessors(fluent = true)
    public static class SchemaReferences {
        final List<SchemaReference> references = new ArrayList<>();
        final Map<String, String> resolvedReferences = new LinkedHashMap<>();

        public void add(final String type, final String subject, final String content) {
            references.add(new SchemaReference(type, subject, -1));
            resolvedReferences.put(subject, content);
        }
    }

    public static final class NamedSchema {
        private final String subject;
        private final ParsedSchema schema;

        public NamedSchema(final String subject, final ParsedSchema schema) {
            this.subject = requireNonNull(subject, "subject");
            this.schema = requireNonNull(schema, "parsedSchema");
        }

        public ParsedSchema schema() {
            return schema;
        }

        public String subject() {
            return subject;
        }
    }
}
