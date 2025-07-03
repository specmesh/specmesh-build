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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Helper for finding schema references within an Avro schema.
 *
 * <p>To be found, a reference to another type must have a {@code subject} defined. Any type with a
 * {@code subject} defined will also be inspected for references.
 *
 * <p>For example, given the schema:
 *
 * <pre>{@code
 * {
 *   "type": "record",
 *   "name": "TypeA",
 *   "fields": [
 *     {"name": "f1", "type": "TypeB", "subject": "type.b.subject"}
 *   ]
 * }
 * }</pre>
 *
 * <p>This class will attempt to load {@code TypeB}, by calling on the {@link SchemaLoader}, and
 * then parse it looking for anymore references.
 *
 * <h2>Namespacing</h2>
 *
 * <p>The `type` field can be a simple type, e.g. {@code TypeB}, or fully qualified, e.g. {@code
 * some.namespace.TypeB}. Unqualified type names will be prefixed with the current namespace, if
 * any.
 *
 * <p>For example, given the schema:
 *
 * <pre>{@code
 * {
 *   "type": "record",
 *   "name": "TypeA",
 *   "namespace": "some.namespace",
 *   "fields": [
 *    {"name": "f1", "type": "TypeB", "subject": "type.b.subject"},
 *    {"name": "f2", "type": "other.namespace.TypeC", "subject": "type.c.subject"}
 *  ]
 * }
 * }</pre>
 *
 * <p>The {@link SchemaLoader} will be invoked for {@code some.namespace.TypeB} and {@code
 * other.namespace.TypeC}.
 *
 * <h2>Error handling</h2>
 *
 * <p>The finder does not try to validate the supplied schemas are valid Avro. That's left to the
 * Avro libraries. However, they must at least be valid JSON, otherwise exceptions will be thrown.
 */
final class AvroReferenceFinder {

    /** Responsible for loading the contents of a type's schema, given the type's name. */
    interface SchemaLoader {

        /**
         * Load the schema content.
         *
         * @param type the name of the type.
         * @return the content of the schema.
         */
        String load(String type);
    }

    private static final JsonMapper MAPPER =
            JsonMapper.builder().enable(JsonParser.Feature.ALLOW_COMMENTS).build();

    private final SchemaLoader schemaLoader;

    /**
     * @param schemaLoader called to load the content of a schema for any referenced types.
     */
    AvroReferenceFinder(final SchemaLoader schemaLoader) {
        this.schemaLoader = requireNonNull(schemaLoader, "schemaLoader");
    }

    /**
     * Find all the schema references in the supplied {@code schema}.
     *
     * @param schemaContent the schema content to check for references.
     * @return an ordered stream of leaf-first referenced schemas, including the supplied {@code
     *     schema}.
     */
    List<DetectedSchema> findReferences(final String schemaContent) {
        final ParsedSchema schema = ParsedSchema.create("", "", schemaContent);

        final String name = schema.name.orElse("");
        final String namespace = schema.namespace.orElse("");
        final String fullyQualifiedName = namespace.isEmpty() ? name : namespace + "." + name;

        final Set<String> visited = new HashSet<>();
        visited.add(fullyQualifiedName);

        return findReferences(schema, visited);
    }

    private List<DetectedSchema> findReferences(
            final ParsedSchema schema, final Set<String> visited) {
        final String type = schema.type.orElse("");
        if (!"record".equals(type)) {
            return List.of(new DetectedSchema(schema, List.of()));
        }

        final List<DetectedSchema> detected =
                schema.nestedTypes.stream()
                        .map(
                                nested ->
                                        findReferences(
                                                ParsedSchema.create(
                                                        nested.name,
                                                        nested.subject,
                                                        loadSchema(nested.name)),
                                                visited))
                        .flatMap(List::stream)
                        .collect(Collectors.toList());

        detected.add(new DetectedSchema(schema, detected));

        return List.copyOf(detected);
    }

    private String loadSchema(final String type) {
        try {
            return Objects.requireNonNull(schemaLoader.load(type), "loader returned null");
        } catch (final Exception e) {
            throw new SchemaLoadException("Failed to load schema for type: " + type, e);
        }
    }

    private static final class NestedType {
        final String name;
        final String subject;

        NestedType(final String name, final String subject) {
            this.name = requireNonNull(name, "name");
            this.subject = requireNonNull(subject, "subject");
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final NestedType that = (NestedType) o;
            return Objects.equals(name, that.name) && Objects.equals(subject, that.subject);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, subject);
        }
    }

    public static final class DetectedSchema {
        private final String name;
        private final String subject;
        private final String content;
        private final List<DetectedSchema> nestedSchemas;

        DetectedSchema(final ParsedSchema source, final List<DetectedSchema> nestedSchemas) {
            this(source.fullName(), source.subject, source.content, nestedSchemas);
        }

        DetectedSchema(
                final String name,
                final String subject,
                final String content,
                final List<DetectedSchema> nestedSchemas) {
            this.name = requireNonNull(name, "name");
            this.subject = requireNonNull(subject, "subject");
            this.content = requireNonNull(content, "content");
            this.nestedSchemas = List.copyOf(requireNonNull(nestedSchemas, "nestedSchemas"));
        }

        public String name() {
            return name;
        }

        public String subject() {
            return subject;
        }

        public String content() {
            return content;
        }

        public List<DetectedSchema> references() {
            return List.copyOf(nestedSchemas);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof DetectedSchema)) {
                return false;
            }
            final DetectedSchema that = (DetectedSchema) o;
            return Objects.equals(name, that.name)
                    && Objects.equals(subject, that.subject)
                    && Objects.equals(content, that.content)
                    && Objects.equals(nestedSchemas, that.nestedSchemas);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, subject, content, nestedSchemas);
        }

        @Override
        public String toString() {
            return "DetectedSchema{"
                    + "name='"
                    + name
                    + '\''
                    + ", subject='"
                    + subject
                    + '\''
                    + ", content='"
                    + content
                    + '\''
                    + ", nestedSchemas="
                    + nestedSchemas
                    + '}';
        }
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private static final class ParsedSchema {

        private final String subject;
        private final String content;
        private final Optional<String> type;
        private final Optional<String> name;
        private final Optional<String> namespace;
        private final List<NestedType> nestedTypes;

        private ParsedSchema(
                final String subject,
                final String content,
                final Optional<String> type,
                final Optional<String> name,
                final Optional<String> namespace,
                final List<NestedType> nestedTypes) {
            this.subject = requireNonNull(subject, "subject");
            this.content = requireNonNull(content, "content");
            this.type = requireNonNull(type, "type");
            this.name = requireNonNull(name, "name");
            this.namespace = requireNonNull(namespace, "namespace");
            this.nestedTypes = requireNonNull(nestedTypes, "nestedTypes");
        }

        public String fullName() {
            if (name.isEmpty()) {
                throw new IllegalStateException("Unnamed schema: " + this);
            }
            return namespace.map(s -> s + "." + name.get()).orElseGet(name::get);
        }

        private static ParsedSchema create(
                final String typeName, final String subject, final String content) {
            try {
                final JsonNode rootNode = MAPPER.readTree(content);
                final Optional<String> type = textChild("type", rootNode);
                final Optional<String> name = textChild("name", rootNode);
                final Optional<String> namespace = textChild("namespace", rootNode);
                final List<NestedType> nestedTypes =
                        findNestedTypesWithSubject(rootNode, namespace.orElse(""));
                return new ParsedSchema(
                        subject,
                        content,
                        type,
                        Optional.of(name.orElse(typeName)),
                        namespace,
                        nestedTypes);
            } catch (final Exception e) {
                throw new InvalidSchemaException(typeName, content, e);
            }
        }

        private static Optional<String> textChild(final String name, final JsonNode node) {
            return Optional.ofNullable(node.get(name))
                    .filter(JsonNode::isTextual)
                    .map(JsonNode::asText);
        }

        private static List<NestedType> findNestedTypesWithSubject(
                final JsonNode node, final String ns) {
            final Optional<String> maybeType =
                    textChild("type", node).filter(text -> !text.isEmpty());

            final Optional<String> maybeSubject =
                    textChild("subject", node).filter(text -> !text.isEmpty());

            if (maybeType.isPresent() && maybeSubject.isPresent()) {
                final String type = maybeType.get();

                if ("array".equals(type)) {
                    return textChild("items", node)
                            .filter(text -> !text.isEmpty())
                            .map(items -> new NestedType(namespaced(items, ns), maybeSubject.get()))
                            .map(List::of)
                            .orElse(List.of());
                }

                if ("map".equals(type)) {
                    return textChild("values", node)
                            .filter(text -> !text.isEmpty())
                            .map(items -> new NestedType(namespaced(items, ns), maybeSubject.get()))
                            .map(List::of)
                            .orElse(List.of());
                }

                return List.of(new NestedType(namespaced(type, ns), maybeSubject.get()));
            }

            final List<NestedType> results = new ArrayList<>(0);
            for (JsonNode child : node) {
                results.addAll(findNestedTypesWithSubject(child, ns));
            }
            return results;
        }

        private static String namespaced(final String type, final String ns) {
            if (ns.isEmpty()) {
                return type;
            }

            final boolean alreadyNamespaced = type.contains(".");
            return alreadyNamespaced ? type : ns + "." + type;
        }
    }

    private static final class InvalidSchemaException extends RuntimeException {

        InvalidSchemaException(final String typeName, final String content, final Exception cause) {
            super(
                    String.format(
                            "Schema content invalid. %scontent: %s", named(typeName), content),
                    cause);
        }

        private static String named(final String typeName) {
            return typeName.isBlank() ? "" : String.format("name: %s, ", typeName);
        }
    }

    private static final class SchemaLoadException extends RuntimeException {
        SchemaLoadException(final String msg, final Throwable cause) {
            super(msg, cause);
        }
    }
}
