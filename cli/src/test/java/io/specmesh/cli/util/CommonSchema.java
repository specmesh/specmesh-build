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

package io.specmesh.cli.util;

import static java.util.stream.Collectors.toMap;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class CommonSchema {

    public static final String OTHER_SCHEMA_SUBJECT = "other.domain.CommonOther";
    public static final String TOPIC_KEY_SCHEMA_SUBJECT =
            "simple.schema_demo._public.some.topic-key";
    public static final String TOPIC_VALUE_SCHEMA_SUBJECT =
            "simple.schema_demo._public.some.topic-value";

    public static final List<Map.Entry<String, List<String>>> COMMON_SCHEMA =
            List.of(
                    Map.entry(OTHER_SCHEMA_SUBJECT, List.of()),
                    Map.entry("other.domain.CommonKey", List.of()),
                    Map.entry("other.domain.Common", List.of(OTHER_SCHEMA_SUBJECT)));

    private static final Path SCHEMA_ROOT = Path.of("./src/test/resources/schema/");

    public static void registerCommonSchema(final SchemaRegistryClient srClient) {
        // Common schema registration covered by
        // https://github.com/specmesh/specmesh-build/issues/453.
        // Until then, handle manually:
        final Map<String, AvroSchema> cache = new HashMap<>();
        COMMON_SCHEMA.forEach(
                e -> buildAndRegisterSchema(e.getKey(), e.getValue(), cache, srClient));
    }

    public static void unregisterCommonSchema(final SchemaRegistryClient srClient) {
        try {
            final Map<String, ArrayList<String>> remaining =
                    COMMON_SCHEMA.stream()
                            .collect(toMap(Map.Entry::getKey, e -> new ArrayList<>(e.getValue())));

            final Set<String> registeredSubjects = Set.copyOf(srClient.getAllSubjects());

            while (!remaining.isEmpty()) {
                final String subject =
                        remaining.entrySet().stream()
                                .filter(e -> e.getValue().isEmpty())
                                .findAny()
                                .map(Map.Entry::getKey)
                                .orElseThrow();

                if (registeredSubjects.contains(subject)) {
                    srClient.deleteSubject(subject);
                }

                remaining.remove(subject);
                remaining.values().forEach(deps -> deps.remove(subject));
            }
        } catch (Exception e) {
            throw new AssertionError("Failed to delete subjects", e);
        }
    }

    private static void buildAndRegisterSchema(
            final String subject,
            final List<String> dependencies,
            final Map<String, AvroSchema> cache,
            final SchemaRegistryClient srClient) {
        cache.computeIfAbsent(
                subject,
                key -> {
                    final List<SchemaReference> references =
                            dependencies.stream()
                                    .map(dep -> new SchemaReference(dep, dep, -1))
                                    .collect(Collectors.toList());

                    final Map<String, String> resolvedReferences =
                            dependencies.stream()
                                    .collect(
                                            toMap(
                                                    Function.identity(),
                                                    sub -> cache.get(sub).canonicalString()));

                    final AvroSchema schema =
                            new AvroSchema(
                                    readLocalSchema(subject, cache),
                                    references,
                                    resolvedReferences,
                                    -1);
                    try {
                        final int id = srClient.register(subject, schema);
                        System.out.println("Registered " + subject + " with id " + id);
                        return schema;
                    } catch (Exception e) {
                        throw new AssertionError("failed to register common schema", e);
                    }
                });
    }

    private static String readLocalSchema(
            final String subject, final Map<String, AvroSchema> cache) {
        final Path path = SCHEMA_ROOT.resolve(subject + ".avsc");
        try {
            return Files.readString(path);
        } catch (IOException e) {
            throw new AssertionError("Failed to read schema: " + path.toAbsolutePath(), e);
        }
    }

    private CommonSchema() {}
}
