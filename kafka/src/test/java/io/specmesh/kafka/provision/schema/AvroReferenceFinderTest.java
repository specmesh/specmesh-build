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
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AvroReferenceFinderTest {

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
                assertThrows(RuntimeException.class, () -> refFinder.findReferences(schema));

        // Then:
        assertThat(e.getMessage(), is("Schema content invalid. content: not-json"));
        assertThat(e.getCause(), is(instanceOf(JsonParseException.class)));
    }

    @Test
    void shouldThrowIfReferencedSchemaCanNotBeParsed() {
        // Given: a -> b
        final String a =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"TypeA\",\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"f1\", \"type\": \"TypeB\", \"subject\":"
                        + " \"type.b.subject\"}\n"
                        + "  ]\n"
                        + "}";

        when(schemaLoader.load(any())).thenReturn("invalid-schema");

        // When:
        final Exception e = assertThrows(RuntimeException.class, () -> refFinder.findReferences(a));

        // Then:
        assertThat(
                e.getMessage(), is("Schema content invalid. name: TypeB, content: invalid-schema"));
        assertThat(e.getCause(), is(instanceOf(JsonParseException.class)));
    }

    @Test
    void shouldReturnSchemaIfJsonButNotAvro() {
        // Given:
        final String schema = "1234";

        // When:
        final List<DetectedSchema> result = refFinder.findReferences(schema);

        // Then:
        assertThat(result, contains(new DetectedSchema("", "", schema, List.of())));
    }

    @Test
    void shouldReturnSchemaIfNoReferences() {
        // Given:
        final String schema =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"SomeType\",\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"f\", \"type\": \"string\"}\n"
                        + "  ]\n"
                        + "}";

        // When:
        final List<DetectedSchema> result = refFinder.findReferences(schema);

        // Then:
        assertThat(result, contains(new DetectedSchema("SomeType", "", schema, List.of())));
    }

    @Test
    void shouldReturnSchemaIfNotRecord() {
        // Given:
        final String schema = "{\n" + "  \"type\": \"int\"\n" + "}";

        // When:
        final List<DetectedSchema> result = refFinder.findReferences(schema);

        // Then:
        assertThat(result, contains(new DetectedSchema("", "", schema, List.of())));
    }

    @Test
    void shouldReturnLeafFirst() {
        // Given: a -> b -> c
        final String a =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"TypeA\",\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"f1\", \"type\": \"TypeB\", \"subject\":"
                        + " \"type.b.subject\"}\n"
                        + "  ]\n"
                        + "}";

        final String b =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"TypeB\",\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"f2\", \"type\": \"TypeC\", \"subject\":"
                        + " \"type.c.subject\"}\n"
                        + "  ]\n"
                        + "}";

        final String c =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"TypeC\",\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"f3\", \"type\": \"string\"}\n"
                        + "  ]\n"
                        + "}";

        when(schemaLoader.load("TypeB")).thenReturn(b);
        when(schemaLoader.load("TypeC")).thenReturn(c);

        // When:
        final List<DetectedSchema> result = refFinder.findReferences(a);

        // Then:
        final DetectedSchema schemaC = new DetectedSchema("TypeC", "type.c.subject", c, List.of());
        final DetectedSchema schemaB =
                new DetectedSchema("TypeB", "type.b.subject", b, List.of(schemaC));
        final DetectedSchema schemaA =
                new DetectedSchema("TypeA", "", a, List.of(schemaC, schemaB));
        assertThat(result, contains(schemaC, schemaB, schemaA));
    }

    @Test
    void shouldSupportFullyQualifiedTypes() {
        // Given: a -> b -> c
        final String a =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"TypeA\",\n"
                        + "  \"namespace\": \"ns.one\",\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"f1\", \"type\": \"TypeB\", \"subject\":"
                        + " \"type.b.subject\"}\n"
                        + "  ]\n"
                        + "}";

        final String b =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"TypeB\",\n"
                        + "  \"namespace\": \"ns.one\",\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"f2\", \"type\": \"ns.two.TypeC\", \"subject\":"
                        + " \"type.c.subject\"}\n"
                        + "  ]\n"
                        + "}";

        final String c =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"TypeC\",\n"
                        + "  \"namespace\": \"ns.two\",\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"f3\", \"type\": \"string\"}\n"
                        + "  ]\n"
                        + "}";

        when(schemaLoader.load("ns.one.TypeB")).thenReturn(b);
        when(schemaLoader.load("ns.two.TypeC")).thenReturn(c);

        // When:
        final List<DetectedSchema> result = refFinder.findReferences(a);

        // Then:
        final DetectedSchema schemaC =
                new DetectedSchema("ns.two.TypeC", "type.c.subject", c, List.of());
        final DetectedSchema schemaB =
                new DetectedSchema("ns.one.TypeB", "type.b.subject", b, List.of(schemaC));
        final DetectedSchema schemaA =
                new DetectedSchema("ns.one.TypeA", "", a, List.of(schemaC, schemaB));
        assertThat(result, contains(schemaC, schemaB, schemaA));
    }

    @Test
    void shouldNotRevisitSameSchemaTwice() {
        // Given: a -> b
        //        | -> b
        final String a =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"TypeA\",\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"f1\", \"type\": \"TypeB\", \"subject\":"
                        + " \"type.b.subject\"},\n"
                        + "    {\"name\": \"f2\", \"type\": \"TypeB\", \"subject\":"
                        + " \"type.b.subject\"}\n"
                        + "  ]\n"
                        + "}";

        final String b =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"TypeB\",\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"f2\", \"type\": \"string\"}\n"
                        + "  ]\n"
                        + "}";

        when(schemaLoader.load("TypeB")).thenReturn(b);

        // When:
        final List<DetectedSchema> result = refFinder.findReferences(a);

        // Then:
        verify(schemaLoader, times(1)).load(any());

        final DetectedSchema schemaB = new DetectedSchema("TypeB", "type.b.subject", b, List.of());
        final DetectedSchema schemaA = new DetectedSchema("TypeA", "", a, List.of(schemaB));
        assertThat(result, contains(schemaB, schemaA));
    }

    @Test
    void shouldNotRevisitSameNestedSchemaTwice() {
        // Given: a -> b -> d
        //        | -> c -> |
        final String a =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"TypeA\",\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"f1\", \"type\": \"TypeB\","
                        + " \"subject\":\"type.b.subject\"},\n"
                        + "    {\"name\": \"f2\", \"type\": \"TypeC\","
                        + " \"subject\":\"type.c.subject\"}\n"
                        + "  ]\n"
                        + "}";

        final String b =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"TypeB\",\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"f1\", \"type\": \"TypeD\","
                        + " \"subject\":\"type.d.subject\"}\n"
                        + "  ]\n"
                        + "}";

        final String c =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"TypeC\",\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"f1\", \"type\": \"TypeD\","
                        + " \"subject\":\"type.d.subject\"}\n"
                        + "  ]\n"
                        + "}";

        final String d =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"TypeD\",\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"f1\", \"type\": \"string\"}\n"
                        + "  ]\n"
                        + "}";

        when(schemaLoader.load("TypeB")).thenReturn(b);
        when(schemaLoader.load("TypeC")).thenReturn(c);
        when(schemaLoader.load("TypeD")).thenReturn(d);

        // When:
        final List<DetectedSchema> result = refFinder.findReferences(a);

        // Then:
        final DetectedSchema schemaD = new DetectedSchema("TypeD", "type.d.subject", d, List.of());
        final DetectedSchema schemaC =
                new DetectedSchema("TypeC", "type.c.subject", c, List.of(schemaD));
        final DetectedSchema schemaB =
                new DetectedSchema("TypeB", "type.b.subject", b, List.of(schemaD));
        final DetectedSchema schemaA =
                new DetectedSchema("TypeA", "", a, List.of(schemaD, schemaB, schemaC));
        assertThat(result, contains(schemaD, schemaB, schemaC, schemaA));
    }

    @Test
    void shouldHandleCircularReferencesByIgnoringThem() {
        // Given: a -> b -> c
        //        a <- |    |
        //             b <- |
        //        a <-------|
        final String a =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"TypeA\",\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"f1\", \"type\": \"TypeB\", \"subject\":"
                        + " \"type.b.subject\"}\n"
                        + "  ]\n"
                        + "}";

        final String b =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"TypeB\",\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"f2\", \"type\": \"TypeC\", \"subject\":"
                        + " \"type.c.subject\"},\n"
                        + "    {\"name\": \"f3\", \"type\": \"TypeA\", \"subject\":"
                        + " \"type.a.subject\"}\n"
                        + "  ]\n"
                        + "}";

        final String c =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"TypeC\",\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"f4\", \"type\": \"TypeB\", \"subject\":"
                        + " \"type.b.subject\"},\n"
                        + "    {\"name\": \"f5\", \"type\": \"TypeA\", \"subject\":"
                        + " \"type.a.subject\"}\n"
                        + "  ]\n"
                        + "}";

        when(schemaLoader.load("TypeB")).thenReturn(b);
        when(schemaLoader.load("TypeC")).thenReturn(c);

        // When:
        final List<DetectedSchema> result = refFinder.findReferences(a);

        // Then:
        final DetectedSchema schemaC = new DetectedSchema("TypeC", "type.c.subject", c, List.of());
        final DetectedSchema schemaB =
                new DetectedSchema("TypeB", "type.b.subject", b, List.of(schemaC));
        final DetectedSchema schemaA =
                new DetectedSchema("TypeA", "", a, List.of(schemaC, schemaB));
        assertThat(result, contains(schemaC, schemaB, schemaA));
        verify(schemaLoader, times(2)).load(any());
    }

    @Test
    void shouldThrowIfReferenceCanNotBeResolved() {
        // Given:
        final String a =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"TypeA\",\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"f1\", \"type\": \"TypeB\", \"subject\":"
                        + " \"type.b.subject\"}\n"
                        + "  ]\n"
                        + "}";

        final RuntimeException cause = mock();
        when(schemaLoader.load("TypeB")).thenThrow(cause);

        // When:
        final Exception e = assertThrows(RuntimeException.class, () -> refFinder.findReferences(a));

        // Then:
        assertThat(e.getMessage(), is("Failed to load schema for type: TypeB"));
        assertThat(e.getCause(), is(sameInstance(cause)));
    }

    @Test
    void shouldPickUpReferencesFromArrays() {
        // Given: a -> b
        final String a =
                "{\n"
                    + "  \"type\": \"record\",\n"
                    + "  \"name\": \"TypeA\",\n"
                    + "  \"fields\": [\n"
                    + "    {\"name\": \"f1\", \"type\": {\"type\": \"array\", \"items\": \"TypeB\","
                    + " \"subject\": \"type.b.subject\"}}\n"
                    + "  ]\n"
                    + "}";

        final String b = "{\n" + "  \"type\": \"int\"\n" + "}";

        when(schemaLoader.load("TypeB")).thenReturn(b);

        // When:
        final List<DetectedSchema> result = refFinder.findReferences(a);

        // Then:
        final DetectedSchema schemaB = new DetectedSchema("TypeB", "type.b.subject", b, List.of());
        final DetectedSchema schemaA = new DetectedSchema("TypeA", "", a, List.of(schemaB));
        assertThat(result, contains(schemaB, schemaA));
    }

    @Test
    void shouldPickUpReferencesFromMaps() {
        // Given: a -> b
        final String a =
                "{\n"
                    + "  \"type\": \"record\",\n"
                    + "  \"name\": \"TypeA\",\n"
                    + "  \"fields\": [\n"
                    + "    {\"name\": \"f1\", \"type\": {\"type\": \"map\", \"values\": \"TypeB\","
                    + " \"subject\": \"type.b.subject\"}}\n"
                    + "  ]\n"
                    + "}";

        final String b = "{\n" + "  \"type\": \"int\"\n" + "}";

        when(schemaLoader.load("TypeB")).thenReturn(b);

        // When:
        final List<DetectedSchema> result = refFinder.findReferences(a);

        // Then:
        final DetectedSchema schemaB = new DetectedSchema("TypeB", "type.b.subject", b, List.of());
        final DetectedSchema schemaA = new DetectedSchema("TypeA", "", a, List.of(schemaB));
        assertThat(result, contains(schemaB, schemaA));
    }

    @Test
    void shouldHandleSchemaWithComments() {
        // Given:
        final String schema =
                "{\n"
                        + "  // a comment"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"SomeType\",\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"f\", \"type\": \"string\"}\n"
                        + "  ]\n"
                        + "}";

        // When:
        refFinder.findReferences(schema);

        // Then: did not throw
    }
}
