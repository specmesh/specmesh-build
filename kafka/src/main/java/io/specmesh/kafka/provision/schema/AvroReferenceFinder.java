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
import static java.util.stream.Collectors.joining;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.util.ClassUtil;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.avro.Schema;

/**
 * Helper for finding external schema references within an Avro schema.
 *
 * <p>Finds any type referred to in the schema, but not defined within the schema. Such external
 * type references are loaded and recursively checked for external schema references.
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
 *    {"name": "f1", "type": "TypeB"},
 *    {"name": "f2", "type": "other.namespace.TypeC"}
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
 * Avro libraries. However, they must at least be valid JSON.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
final class AvroReferenceFinder {

    /**
     * @param typeName the fully qualified name of the type in the schema file.
     * @param content the contents of the schema file.
     * @param location where the schema was loaded from, as returned by the {@link SchemaLoader}.
     * @param references the external references the schema contains.
     */
    record DetectedSchema(
            String typeName, String content, String location, List<DetectedSchema> references) {
        public DetectedSchema {
            requireNonNull(typeName, "typeName");
            requireNonNull(content, "content");
            requireNonNull(location, "location");
            references = List.copyOf(requireNonNull(references, "references"));
        }
    }

    record LoadedSchema(String path, String content) {
        LoadedSchema {
            requireNonNull(path, "path");
            requireNonNull(content, "content");
        }
    }

    /** Responsible for loading the contents of a type's schema, given the type's name. */
    interface SchemaLoader {

        /**
         * Load the schema content.
         *
         * @param type the fully-qualified name of the type.
         * @return the content of the schema.
         */
        LoadedSchema load(String type);
    }

    private static final JsonMapper MAPPER =
            JsonMapper.builder().enable(JsonParser.Feature.ALLOW_COMMENTS).build();

    private static final Map<String, Schema.Type> STD_TYPE_NAMES =
            Arrays.stream(Schema.Type.values())
                    .filter(type -> type != Schema.Type.UNION)
                    .collect(
                            Collectors.toUnmodifiableMap(
                                    type -> type.toString().toLowerCase(), type -> type));

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
     * @param schemaPath the path to the schema file.
     * @param schemaContent the schema content to check for references.
     * @return an ordered stream of leaf-first referenced external schemas, including the supplied
     *     {@code schema}.
     */
    List<DetectedSchema> findReferences(final String schemaPath, final String schemaContent) {
        final Route route = new Route(schemaPath);
        final Map<TypeName, List<DetectedSchema>> visitedTypes = new ConcurrentHashMap<>();
        return findReferences(route, schemaContent, Optional.empty(), visitedTypes);
    }

    private List<DetectedSchema> findReferences(
            final Route route,
            final String schemaContent,
            final Optional<TypeName> knownTypeName,
            final Map<TypeName, List<DetectedSchema>> visitedTypes) {
        final SchemaInfo schema = parseSchema(route, schemaContent, knownTypeName);
        schema.name.ifPresent(name -> visitedTypes.put(name, List.of()));

        final List<DetectedSchema> externalRefs =
                schema.externalReferences().stream()
                        .map(
                                typeRef -> {
                                    final List<DetectedSchema> existing = visitedTypes.get(typeRef);
                                    if (existing != null) {
                                        return existing;
                                    }

                                    final LoadedSchema loaded =
                                            loadSchema(typeRef.fullyQualifiedName(), route);

                                    return findReferences(
                                            route.push(loaded.path()),
                                            loaded.content(),
                                            Optional.of(typeRef),
                                            visitedTypes);
                                })
                        .flatMap(List::stream)
                        .distinct()
                        .toList();

        final List<DetectedSchema> detected = new ArrayList<>(externalRefs);
        detected.add(
                new DetectedSchema(
                        schema.name().map(TypeName::fullyQualifiedName).orElse(""),
                        schema.content(),
                        route.current(),
                        externalRefs));
        final List<DetectedSchema> immutable = List.copyOf(detected);
        schema.name.ifPresent(name -> visitedTypes.put(name, immutable));
        return immutable;
    }

    private SchemaInfo parseSchema(
            final Route route, final String content, final Optional<TypeName> expectedName) {
        try {
            final JsonNode rootNode = MAPPER.readTree(content);

            final Optional<TypeName> actualName = namedTypeName(rootNode);

            if (expectedName.isPresent()) {
                if (actualName.isEmpty()) {
                    throw new IllegalArgumentException(
                            "Not a named type. Avro only supports named types, e.g. record, fixed,"
                                    + " enum, in external schema.");
                }

                if (!expectedName.get().equals(actualName.get())) {
                    throw new IllegalArgumentException(
                            "Expected schema file to contain type '%s', but contained '%s'"
                                    .formatted(expectedName.get(), actualName.get()));
                }
            }

            final TypeCollector typeCollector = new TypeCollector();
            typeCollector.collect(rootNode);

            return new SchemaInfo(
                    route.current(), content, actualName, typeCollector.externalReferences);
        } catch (final Exception e) {
            throw new InvalidSchemaException(route, content, e);
        }
    }

    private static Optional<String> textChild(final String name, final JsonNode node) {
        return Optional.ofNullable(node.get(name))
                .filter(JsonNode::isTextual)
                .map(JsonNode::asText);
    }

    private static TypeInfo type(final JsonNode typeNode, final String currentNamespace) {
        if (typeNode.isArray()) {
            return TypeInfo.stdType(Schema.Type.UNION);
        }

        if (typeNode.isTextual()) {
            final String typeName = typeNode.asText();
            final Schema.Type stdType = STD_TYPE_NAMES.get(typeName);
            return stdType != null
                    ? TypeInfo.stdType(stdType)
                    : TypeInfo.typeReference(typeName, currentNamespace);
        }

        return textChild("type", typeNode)
                .map(String::toUpperCase)
                .flatMap(
                        name -> {
                            try {
                                return Optional.of(Schema.Type.valueOf(name));
                            } catch (final Exception e) {
                                return Optional.empty();
                            }
                        })
                .map(TypeInfo::stdType)
                .orElseGet(TypeInfo::empty);
    }

    private static final class TypeCollector {
        private final Set<TypeName> definedTypes = new HashSet<>();
        private final List<TypeName> externalReferences = new ArrayList<>(0);

        void collect(final JsonNode rootNode) {
            findTypes(rootNode, "");
        }

        private void findTypes(final JsonNode node, final String currentNamespace) {
            final TypeInfo type = type(node, currentNamespace);
            if (type.referencedType().isPresent()) {
                final TypeName referencedType = type.referencedType().get();
                if (!definedTypes.contains(referencedType)
                        && !externalReferences.contains(referencedType)) {
                    externalReferences.add(referencedType);
                }
            }

            if (type.stdType().isPresent()) {
                switch (type.stdType().get()) {
                    case RECORD -> handleRecord(node, currentNamespace);
                    case ARRAY -> handleArray(node, currentNamespace);
                    case MAP -> handleMap(node, currentNamespace);
                    case UNION -> handleUnion(node, currentNamespace);
                    case ENUM, FIXED -> handleNamedType(node, currentNamespace);
                    default -> {}
                }
            }
        }

        private String handleNamedType(final JsonNode node, final String currentNamespace) {
            final String name = textChild("name", node).orElse("");
            final String namespace = textChild("namespace", node).orElse(currentNamespace);

            definedTypes.add(new TypeName(namespace, name));

            return namespace;
        }

        private void handleRecord(final JsonNode node, final String currentNamespace) {
            final String namespace = handleNamedType(node, currentNamespace);

            final Iterator<JsonNode> fields =
                    Optional.ofNullable(node.get("fields"))
                            .map(JsonNode::elements)
                            .orElse(ClassUtil.emptyIterator());

            while (fields.hasNext()) {
                final JsonNode field = fields.next();
                Optional.ofNullable(field.get("type"))
                        .ifPresent(type -> findTypes(type, namespace));
            }
        }

        private void handleArray(final JsonNode node, final String currentNamespace) {
            final JsonNode items = node.get("items");
            if (items != null) {
                findTypes(items, currentNamespace);
            }
        }

        private void handleMap(final JsonNode node, final String currentNamespace) {
            final JsonNode values = node.get("values");
            if (values != null) {
                findTypes(values, currentNamespace);
            }
        }

        private void handleUnion(final JsonNode node, final String currentNamespace) {
            for (JsonNode unionType : node) {
                findTypes(unionType, currentNamespace);
            }
        }
    }

    private static Optional<TypeName> namedTypeName(final JsonNode rootNode) {
        final Optional<Schema.Type> namedType =
                type(rootNode, "")
                        .stdType()
                        .filter(
                                type ->
                                        type == Schema.Type.RECORD
                                                || type == Schema.Type.ENUM
                                                || type == Schema.Type.FIXED);

        if (namedType.isEmpty()) {
            return Optional.empty();
        }

        final String namespace = textChild("namespace", rootNode).orElse("");
        final String name = textChild("name", rootNode).orElse("");
        return Optional.of(new TypeName(namespace, name));
    }

    private LoadedSchema loadSchema(final String type, final Route route) {
        try {
            return Objects.requireNonNull(schemaLoader.load(type), "loader returned null");
        } catch (final Exception e) {
            throw new SchemaLoadException(type, route, e);
        }
    }

    private record SchemaInfo(
            String schemaPath,
            String content,
            Optional<TypeName> name,
            List<TypeName> externalReferences) {}

    private record TypeName(String namespace, String name) {
        TypeName {
            requireNonNull(namespace, "namespace");
            requireNonNull(name, "name");

            if (name.isEmpty()) {
                throw new IllegalArgumentException("name can not be empty");
            }
        }

        String fullyQualifiedName() {
            return namespace.isEmpty() ? name : "%s.%s".formatted(namespace, name);
        }

        @Override
        public String toString() {
            return fullyQualifiedName();
        }
    }

    private record TypeInfo(Optional<Schema.Type> stdType, Optional<TypeName> referencedType) {

        private static final TypeInfo EMPTY = new TypeInfo(Optional.empty(), Optional.empty());

        TypeInfo {
            requireNonNull(stdType, "stdType");
            requireNonNull(referencedType, "referencedType");
        }

        static TypeInfo empty() {
            return EMPTY;
        }

        static TypeInfo stdType(final Schema.Type stdType) {
            return new TypeInfo(Optional.of(stdType), Optional.empty());
        }

        static TypeInfo typeReference(final String referencedType, final String currentNamespace) {
            return new TypeInfo(
                    Optional.empty(), Optional.of(parseTypeName(referencedType, currentNamespace)));
        }

        private static TypeName parseTypeName(final String type, final String currentNamespace) {
            final int nsEnd = type.lastIndexOf('.');
            return nsEnd < 0
                    ? new TypeName(currentNamespace, type)
                    : new TypeName(type.substring(0, nsEnd), type.substring(nsEnd + 1));
        }
    }

    private record Route(List<String> paths) {

        Route(final String initial) {
            this(new ArrayList<>(List.of(initial)));
        }

        Route push(final String schemaPath) {
            final Route copy = new Route(new ArrayList<>(paths));
            copy.paths.add(schemaPath);
            return copy;
        }

        private String current() {
            return paths.get(paths.size() - 1);
        }

        @Override
        public String toString() {
            return paths.stream().map(" -> %s"::formatted).collect(joining()).substring(4);
        }
    }

    private static final class InvalidSchemaException extends RuntimeException {
        InvalidSchemaException(final Route route, final String content, final Exception cause) {
            super(
                    "Schema content invalid. schema file chain: %s, content: %s"
                            .formatted(route, content),
                    cause);
        }
    }

    @VisibleForTesting
    static final class SchemaLoadException extends RuntimeException {
        SchemaLoadException(final String type, final Route route, final Throwable cause) {
            super(
                    "Failed to load schema for type: %s, referenced via schema file chain: %s"
                            .formatted(type, route),
                    cause);
        }
    }
}
