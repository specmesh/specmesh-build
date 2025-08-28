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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonParseException;
import io.specmesh.kafka.provision.schema.AvroReferenceFinder.DetectedSchema;
import io.specmesh.kafka.provision.schema.AvroReferenceFinder.LoadedSchema;
import java.util.List;
import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AvroReferenceFinderTest {

    private static final String ROOT_SCHEMA_PATH = "some/path/to/root-schema.avsc";
    private static final String NESTED_SCHEMA_PATH = "some/path/to/nested-schema.avsc";

    @Mock private AvroReferenceFinder.SchemaLoader schemaLoader;
    private AvroReferenceFinder refFinder;

    @BeforeEach
    void setUp() {
        refFinder = new AvroReferenceFinder(schemaLoader);
    }

    @Test
    void shouldThrowIfSchemaCanNotBeParsed() {
        // Given:
        final String schema = "not-json";

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class,
                        () -> refFinder.findReferences(ROOT_SCHEMA_PATH, schema));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "Schema content invalid. schemaPath: some/path/to/root-schema.avsc,"
                                + " content: not-json"));
        assertThat(e.getCause(), is(instanceOf(JsonParseException.class)));
    }

    @Test
    void shouldThrowIfReferencedSchemaCanNotBeParsed() {
        // Given: a -> b
        final String a =
                """
                {
                  "type": "record",
                  "name": "TypeA",
                  "fields": [
                    {"name": "f1", "type": "TypeB"}
                  ]
                }""";

        when(schemaLoader.load(any())).thenReturn(loadedSchema("invalid-schema"));

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class,
                        () -> refFinder.findReferences(ROOT_SCHEMA_PATH, a));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "Schema content invalid. schemaPath: some/path/to/nested-schema.avsc,"
                                + " content: invalid-schema"));
        assertThat(e.getCause(), is(instanceOf(JsonParseException.class)));
    }

    @Test
    void shouldReturnSchemaIfJsonButNotAvro() {
        // Given:
        final String schema = "1234";

        // When:
        final List<DetectedSchema> result = refFinder.findReferences(ROOT_SCHEMA_PATH, schema);

        // Then:
        assertThat(result, contains(new DetectedSchema("", schema, List.of())));
    }

    @Test
    void shouldReturnPrimitiveSchema() {
        // Given:
        final String schema =
                ensureValidAvro(
                        """
                {
                  "type": "int"
                }
                """);

        // When:
        final List<DetectedSchema> result = refFinder.findReferences(ROOT_SCHEMA_PATH, schema);

        // Then:
        assertThat(result, contains(new DetectedSchema("", schema, List.of())));
    }

    @Test
    void shouldSupportSimpleRecord() {
        // Given:
        final String schema =
                ensureValidAvro(
                        """
            {
              "type": "record",
              "name": "SomeType",
              "fields": [
                {"name": "f", "type": "string"}
              ]
            }""");

        // When:
        final List<DetectedSchema> result = refFinder.findReferences(ROOT_SCHEMA_PATH, schema);

        // Then:
        assertThat(result, contains(new DetectedSchema("SomeType", schema, List.of())));
    }

    @Test
    void shouldSupportSimpleNamespacedRecord() {
        // Given:
        final String schema =
                ensureValidAvro(
                        """
            {
              "type": "record",
              "name": "SomeType",
              "namespace": "some.namespace",
              "fields": [
                {"name": "f", "type": "string"}
              ]
            }""");

        // When:
        final List<DetectedSchema> result = refFinder.findReferences(ROOT_SCHEMA_PATH, schema);

        // Then:
        assertThat(
                result, contains(new DetectedSchema("some.namespace.SomeType", schema, List.of())));
    }

    @Test
    void shouldSupportInlinedTypes() {
        // Given:
        final String a =
                ensureValidAvro(
                        """
                {
                  "type": "record",
                  "name": "TypeA",
                  "namespace": "ns.one",
                  "fields": [
                    {
                      "name": "f1",
                      "type": {
                        "type": "fixed",
                        "size": 16,
                        "name": "WithoutExplicitNamespace"
                      }
                    },
                    {
                      "name": "f2",
                      "type": "WithoutExplicitNamespace"
                    },
                    {
                      "name": "f3",
                      "type": {
                        "type": "map",
                        "values": "ns.one.WithoutExplicitNamespace"
                      }
                    },
                    {
                      "name": "f4",
                      "type": {
                        "type": "enum",
                        "name": "WithExplicitNamespace",
                        "namespace": "ns.two",
                        "symbols" : ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
                      }
                    },
                    {
                      "name": "f5",
                      "type": {
                        "type": "array",
                        "items": "ns.two.WithExplicitNamespace"
                      }
                    }
                  ]
                }
                """);

        // When:
        final List<DetectedSchema> result = refFinder.findReferences(ROOT_SCHEMA_PATH, a);

        // Then:
        final DetectedSchema schemaA = new DetectedSchema("ns.one.TypeA", a, List.of());
        assertThat(result, contains(schemaA));
    }

    @Test
    void shouldSupportReferencedTypeReturningLeafFirst() {
        // Given: a -> b
        //        | -> c
        final String c =
                ensureValidAvro(
                        """
                {
                  "type": "enum",
                  "name": "TypeC",
                  "namespace": "ns.one",
                  "symbols" : ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
                }
                """);

        final String b =
                ensureValidAvro(
                        c,
                        """
                {
                  "type": "record",
                  "name": "TypeB",
                  "namespace": "ns.one",
                  "fields": [
                    {
                      "name": "f3",
                      "type": [
                        "null",
                        {
                          "type": "array",
                          "items": "TypeC"
                        }
                      ]
                    }
                  ]
                }
                """);

        final String a =
                ensureValidAvro(
                        c,
                        b,
                        """
                {
                  "type": "record",
                  "name": "TypeA",
                  "namespace": "ns.one",
                  "fields": [
                    {
                      "name": "f1",
                      "type": {
                        "type": "map",
                        "values": "TypeB"
                      }
                    }
                  ]
                }
                """);

        when(schemaLoader.load("ns.one.TypeB")).thenReturn(loadedSchema(b));
        when(schemaLoader.load("ns.one.TypeC")).thenReturn(loadedSchema(c));

        // When:
        final List<DetectedSchema> result = refFinder.findReferences(ROOT_SCHEMA_PATH, a);

        // Then:
        final DetectedSchema schemaC = new DetectedSchema("ns.one.TypeC", c, List.of());
        final DetectedSchema schemaB = new DetectedSchema("ns.one.TypeB", b, List.of(schemaC));
        final DetectedSchema schemaA =
                new DetectedSchema("ns.one.TypeA", a, List.of(schemaC, schemaB));
        assertThat(result, contains(schemaC, schemaB, schemaA));
    }

    @Test
    void shouldSupportFullyQualifiedReferencedTypes() {
        // Given: a -> b
        //        | -> c
        final String c =
                ensureValidAvro(
                        """
                {
                  "type": "fixed",
                  "size": 16,
                  "name": "TypeC",
                  "namespace": "ns.one"
                }
                """);

        final String b =
                ensureValidAvro(
                        c,
                        """
                {
                  "type": "record",
                  "name": "TypeB",
                  "namespace": "ns.two",
                  "fields": [
                    {
                      "name": "f3",
                      "type": [
                        "null",
                        {
                          "type": "array",
                          "items": "ns.one.TypeC"
                        }
                      ]
                    }
                  ]
                }
                """);

        final String a =
                ensureValidAvro(
                        c,
                        b,
                        """
                {
                  "type": "record",
                  "name": "TypeA",
                  "namespace": "ns.one",
                  "fields": [
                    {
                      "name": "f1",
                      "type": {
                        "type": "record",
                        "name": "bob",
                        "fields": [
                          {
                            "name": "f2",
                            "type": {
                              "type": "map",
                              "values": "ns.two.TypeB"
                            }
                          }
                        ]
                      }
                    }
                  ]
                }
                """);

        when(schemaLoader.load("ns.two.TypeB")).thenReturn(loadedSchema(b));
        when(schemaLoader.load("ns.one.TypeC")).thenReturn(loadedSchema(c));

        // When:
        final List<DetectedSchema> result = refFinder.findReferences(ROOT_SCHEMA_PATH, a);

        // Then:
        final DetectedSchema schemaC = new DetectedSchema("ns.one.TypeC", c, List.of());
        final DetectedSchema schemaB = new DetectedSchema("ns.two.TypeB", b, List.of(schemaC));
        final DetectedSchema schemaA =
                new DetectedSchema("ns.one.TypeA", a, List.of(schemaC, schemaB));
        assertThat(result, contains(schemaC, schemaB, schemaA));
    }

    @Test
    void shouldSupportRecordsWithoutNamespaces() {
        // Given: a -> b
        final String b =
                ensureValidAvro(
                        """
                {
                  "type": "fixed",
                  "size": 16,
                  "name": "TypeB"
                }
                """);

        final String a =
                ensureValidAvro(
                        b,
                        """
                {
                  "type": "record",
                  "name": "TypeA",
                  "fields": [
                    {"name": "f1", "type": "TypeB"}
                  ]
                }
                """);

        when(schemaLoader.load("TypeB")).thenReturn(loadedSchema(b));

        // When:
        final List<DetectedSchema> result = refFinder.findReferences(ROOT_SCHEMA_PATH, a);

        // Then:
        final DetectedSchema schemaB = new DetectedSchema("TypeB", b, List.of());
        final DetectedSchema schemaA = new DetectedSchema("TypeA", a, List.of(schemaB));
        assertThat(result, contains(schemaB, schemaA));
    }

    @Test
    void shouldSupportStarDependencies() {
        // Given: a -> b -> d
        //        | -> c -> |
        final String d =
                ensureValidAvro(
                        """
                {
                  "type": "record",
                  "name": "TypeD",
                  "namespace": "ns.one",
                  "fields": [
                    {"name": "f5", "type": "string"}
                  ]
                }
                """);

        final String c =
                ensureValidAvro(
                        d,
                        """
                {
                  "type": "record",
                  "name": "TypeC",
                  "namespace": "ns.one",
                  "fields": [
                    {"name": "f4", "type": "TypeD"}
                  ]
                }
                """);

        final String b =
                ensureValidAvro(
                        d,
                        """
                {
                  "type": "record",
                  "name": "TypeB",
                  "namespace": "ns.one",
                  "fields": [
                    {"name": "f3", "type": "TypeD"}
                  ]
                }
                """);

        final String a =
                ensureValidAvro(
                        d,
                        c,
                        b,
                        """
                {
                  "type": "record",
                  "name": "TypeA",
                  "namespace": "ns.one",
                  "fields": [
                    {"name": "f1", "type": "TypeB"},
                    {"name": "f2", "type": "TypeC"}
                  ]
                }
                """);

        when(schemaLoader.load("ns.one.TypeB")).thenReturn(loadedSchema(b));
        when(schemaLoader.load("ns.one.TypeC")).thenReturn(loadedSchema(c));
        when(schemaLoader.load("ns.one.TypeD")).thenReturn(loadedSchema(d));

        // When:
        final List<DetectedSchema> result = refFinder.findReferences(ROOT_SCHEMA_PATH, a);

        // Then:
        final DetectedSchema schemaD = new DetectedSchema("ns.one.TypeD", d, List.of());
        final DetectedSchema schemaC = new DetectedSchema("ns.one.TypeC", c, List.of(schemaD));
        final DetectedSchema schemaB = new DetectedSchema("ns.one.TypeB", b, List.of(schemaD));
        final DetectedSchema schemaA =
                new DetectedSchema("ns.one.TypeA", a, List.of(schemaD, schemaB, schemaC));
        assertThat(result, contains(schemaD, schemaB, schemaC, schemaA));
    }

    @Test
    void shouldNotRevisitSameSchemaTwice() {
        // Given: a -> b
        //        | -> b
        final String b =
                ensureValidAvro(
                        """
                        {
                          "type": "record",
                          "name": "TypeB",
                          "fields": [
                            {"name": "f2", "type": "string"}
                          ]
                        }""");

        final String a =
                ensureValidAvro(
                        b,
                        """
                        {
                          "type": "record",
                          "name": "TypeA",
                          "fields": [
                            {"name": "f1", "type": "TypeB"},
                            {"name": "f2", "type": "TypeB"}
                          ]
                        }""");

        when(schemaLoader.load("TypeB")).thenReturn(loadedSchema(b));

        // When:
        final List<DetectedSchema> result = refFinder.findReferences(ROOT_SCHEMA_PATH, a);

        // Then:
        verify(schemaLoader, times(1)).load(any());

        final DetectedSchema schemaB = new DetectedSchema("TypeB", b, List.of());
        final DetectedSchema schemaA = new DetectedSchema("TypeA", a, List.of(schemaB));
        assertThat(result, contains(schemaB, schemaA));
    }

    @Test
    void shouldHandleInternalCircularReferences() {
        // Given:
        final String a =
                ensureValidAvro(
                        """
                        {
                          "type": "record",
                          "name": "TypeA",
                          "fields": [
                            {"name": "f1", "type": "string"},
                            {"name": "next", "type": ["null","TypeA"]}
                          ]
                        }""");

        // When:
        final List<DetectedSchema> result = refFinder.findReferences(ROOT_SCHEMA_PATH, a);

        // Then:
        final DetectedSchema schemaA = new DetectedSchema("TypeA", a, List.of());
        assertThat(result, contains(schemaA));
    }

    @Test
    void shouldHandleCircularReferencesByIgnoringThemAsTheyCanNoBeRegistered() {
        // Given: a -> b -> c
        //        a <- |    |
        //             b <- |
        //        a <-------|
        final String a =
                """
                        {
                          "type": "record",
                          "name": "TypeA",
                          "fields": [
                            {"name": "f1", "type": "TypeB"}
                          ]
                        }""";

        final String b =
                """
                        {
                          "type": "record",
                          "name": "TypeB",
                          "fields": [
                            {"name": "f2", "type": "TypeC"},
                            {"name": "f3", "type": "TypeA"}
                          ]
                        }""";

        final String c =
                """
                        {
                          "type": "record",
                          "name": "TypeC",
                          "fields": [
                            {"name": "f4", "type": "TypeB"},
                            {"name": "f5", "type": "TypeA"}
                          ]
                        }""";

        when(schemaLoader.load("TypeB")).thenReturn(loadedSchema(b));
        when(schemaLoader.load("TypeC")).thenReturn(loadedSchema(c));

        // When:
        final List<DetectedSchema> result = refFinder.findReferences(ROOT_SCHEMA_PATH, a);

        // Then:
        final DetectedSchema schemaC = new DetectedSchema("TypeC", c, List.of());
        final DetectedSchema schemaB = new DetectedSchema("TypeB", b, List.of(schemaC));
        final DetectedSchema schemaA = new DetectedSchema("TypeA", a, List.of(schemaC, schemaB));
        assertThat(result, contains(schemaC, schemaB, schemaA));
        verify(schemaLoader, times(2)).load(any());
    }

    @Test
    void shouldThrowIfReferenceCanNotBeResolved() {
        // Given:
        final String a =
                """
                        {
                          "type": "record",
                          "name": "TypeA",
                          "fields": [
                            {"name": "f1", "type": "TypeB"}
                          ]
                        }""";

        final RuntimeException cause = mock();
        when(schemaLoader.load("TypeB")).thenThrow(cause);

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class,
                        () -> refFinder.findReferences(ROOT_SCHEMA_PATH, a));

        // Then:
        assertThat(e.getMessage(), is("Failed to load schema for type: TypeB"));
        assertThat(e.getCause(), is(sameInstance(cause)));
    }

    @Test
    void shouldHandleSchemaWithComments() {
        // Given:
        final String schema =
                ensureValidAvro(
                        """
                        {
                          // a comment
                          "type": "record",
                          "name": "SomeType",
                          "fields": [
                            {"name": "f", "type": "string"}
                          ]
                        }""");

        // When:
        refFinder.findReferences(ROOT_SCHEMA_PATH, schema);

        // Then: did not throw
    }

    private static String ensureValidAvro(final String... schema) {
        final Schema.Parser parser = new Schema.Parser();
        for (final String s : schema) {
            parser.parse(s);
        }
        return schema[schema.length - 1];
    }

    private static LoadedSchema loadedSchema(final String content) {
        return new LoadedSchema(NESTED_SCHEMA_PATH, content);
    }
}
