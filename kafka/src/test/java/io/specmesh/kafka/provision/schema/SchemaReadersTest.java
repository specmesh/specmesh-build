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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.specmesh.kafka.provision.schema.AvroReferenceFinder.SchemaLoadException;
import io.specmesh.kafka.provision.schema.SchemaProvisioner.SchemaProvisioningException;
import io.specmesh.kafka.provision.schema.SchemaReaders.NamedSchema;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SchemaReadersTest {

    @Nested
    class ClassPathSchemaReaderTest {
        private SchemaReaders.ClassPathSchemaReader reader;

        @BeforeEach
        void setUp() {
            reader = new SchemaReaders.ClassPathSchemaReader();
        }

        @Test
        void shouldReadSchemaContentFromClassPath() {
            // When:
            final String content = reader.readContent("schema/other.domain.Common.avsc");

            // Then:
            assertThat(
                    content,
                    containsString(
                            """
                            "name": "Common"\
                            """));
        }

        @Test
        void shouldLoadedReferencedSchemaFromRootFolderInClassPath() {
            // When:
            final List<NamedSchema> result = reader.read("a.domain.Root.avsc");

            // Then:
            assertThat(result, hasSize(2));
            assertThat(result.get(0).subject(), is("a.domain.Thing"));
            assertThat(result.get(0).location(), is("a.domain.Thing.avsc"));
            assertThat(result.get(0).schema().name(), containsString("a.domain.Thing"));
            assertThat(result.get(1).subject(), is("a.domain.Root"));
            assertThat(result.get(1).location(), is("a.domain.Root.avsc"));
            assertThat(result.get(1).schema().name(), containsString("a.domain.Root"));
        }

        @Test
        void shouldLoadedReferencedSchemaFromSubFolderInClassPath() {
            // When:
            final List<NamedSchema> result = reader.read("schema/other.domain.Root.avsc");

            // Then:
            assertThat(result, hasSize(2));
            assertThat(result.get(0).subject(), is("other.domain.Common"));
            assertThat(result.get(0).schema().name(), containsString("other.domain.Common"));
            assertThat(result.get(0).location(), is("schema/other.domain.Common.avsc"));
            assertThat(result.get(1).subject(), is("other.domain.Root"));
            assertThat(result.get(1).location(), is("schema/other.domain.Root.avsc"));
            assertThat(result.get(1).schema().name(), containsString("other.domain.Root"));
        }

        @Test
        void shouldThrowIfRootSchemaDoesNotExist() {
            // When:
            final Exception e =
                    assertThrows(
                            SchemaProvisioningException.class,
                            () -> reader.read("schema/i.do.not.Exist.avsc"));

            // Then:
            assertThat(
                    e.getMessage(),
                    is("Failed to read schema from classpath:schema/i.do.not.Exist.avsc"));
            assertThat(e.getCause().getMessage(), is("schema/i.do.not.Exist.avsc not found"));
        }

        @Test
        void shouldThrowIfNestedSchemaDoesNotExist() {
            // When:
            final Exception e =
                    assertThrows(
                            SchemaLoadException.class,
                            () -> reader.read("schema/other.domain.Bad.avsc"));

            // Then:
            assertThat(
                    e.getMessage(),
                    is(
                            "Failed to load schema for type: some.unknown.Type, referenced via"
                                    + " schema file chain: schema/other.domain.Bad.avsc"));
            assertThat(e.getCause(), is(instanceOf(SchemaProvisioningException.class)));
            assertThat(
                    e.getCause().getMessage(),
                    is("Failed to read schema from classpath:schema/some.unknown.Type.avsc"));
        }
    }

    @Nested
    class FileSystemSchemaReaderTest {
        private SchemaReaders.FileSystemSchemaReader reader;
        private Path resourcesDir;

        @BeforeEach
        void setUp() {
            reader = new SchemaReaders.FileSystemSchemaReader();
            resourcesDir = Path.of("src/test/resources");
        }

        @Test
        void shouldReadSchemaContentFromClassPath() {
            // When:
            final String content =
                    reader.readContent(resourcesDir.resolve("schema/other.domain.Common.avsc"));

            // Then:
            assertThat(
                    content,
                    containsString(
                            """
                            "name": "Common"\
                            """));
        }

        @Test
        void shouldLoadedReferencedSchemaFromRootFolderInClassPath() {
            // Given:
            final Path rootPath = resourcesDir.resolve("a.domain.Root.avsc");
            final Path thingPath = resourcesDir.resolve("a.domain.Thing.avsc");

            // When:
            final List<NamedSchema> result = reader.read(rootPath);

            // Then:
            assertThat(result, hasSize(2));
            assertThat(result.get(0).subject(), is("a.domain.Thing"));
            assertThat(result.get(0).location(), is(thingPath.toAbsolutePath().toString()));
            assertThat(result.get(0).schema().name(), containsString("a.domain.Thing"));
            assertThat(result.get(1).subject(), is("a.domain.Root"));
            assertThat(result.get(1).location(), is(rootPath.toAbsolutePath().toString()));
            assertThat(result.get(1).schema().name(), containsString("a.domain.Root"));
        }

        @Test
        void shouldLoadedReferencedSchemaFromSubFolderInClassPath() {
            // Given:
            final Path rootPath = resourcesDir.resolve("schema/other.domain.Root.avsc");
            final Path thingPath = resourcesDir.resolve("schema/other.domain.Common.avsc");

            // When:
            final List<NamedSchema> result = reader.read(rootPath);

            // Then:
            assertThat(result, hasSize(2));
            assertThat(result.get(0).subject(), is("other.domain.Common"));
            assertThat(result.get(0).location(), is(thingPath.toAbsolutePath().toString()));
            assertThat(result.get(0).schema().name(), containsString("other.domain.Common"));
            assertThat(result.get(1).subject(), is("other.domain.Root"));
            assertThat(result.get(1).location(), is(rootPath.toAbsolutePath().toString()));
            assertThat(result.get(1).schema().name(), containsString("other.domain.Root"));
        }

        @Test
        void shouldThrowIfRootSchemaDoesNotExist() {
            // Given:
            final Path path = resourcesDir.resolve("schema/i.do.not.Exist.avsc");

            // When:
            final Exception e =
                    assertThrows(SchemaProvisioningException.class, () -> reader.read(path));

            // Then:
            assertThat(
                    e.getMessage(),
                    is("Failed to read schema at path:" + path.toAbsolutePath().normalize()));
            assertThat(e.getCause(), is(instanceOf(NoSuchFileException.class)));
            assertThat(e.getCause().getMessage(), is(path.toAbsolutePath().normalize().toString()));
        }

        @Test
        void shouldThrowIfNestedSchemaDoesNotExist() {
            // Given:
            final Path path = resourcesDir.resolve("schema/other.domain.Bad.avsc");

            // When:
            final Exception e = assertThrows(SchemaLoadException.class, () -> reader.read(path));

            // Then:
            assertThat(
                    e.getMessage(),
                    is(
                            "Failed to load schema for type: some.unknown.Type, referenced via"
                                    + " schema file chain: "
                                    + path.toAbsolutePath().normalize()));
            assertThat(e.getCause(), is(instanceOf(SchemaProvisioningException.class)));
            assertThat(
                    e.getCause().getMessage(),
                    is(
                            "Failed to read schema at path:"
                                    + resourcesDir
                                            .resolve("schema/some.unknown.Type.avsc")
                                            .toAbsolutePath()
                                            .normalize()));
        }
    }
}
