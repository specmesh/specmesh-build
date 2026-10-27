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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.specmesh.kafka.provision.schema.SubjectModes.SubjectModeException;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SubjectModesTest {

    private static final String SUBJECT = "some.topic-key";

    @Mock private SchemaRegistryClient client;

    private SubjectModes modes;

    @BeforeEach
    void setUp() {
        modes = new SubjectModes(client);
    }

    @Test
    void shouldGetSubjectMode() throws Exception {
        // Given:
        when(client.getMode(SUBJECT)).thenReturn("READONLY");

        // When:
        final Optional<SubjectMode> mode = modes.get(SUBJECT);

        // Then:
        assertThat(mode, is(Optional.of(SubjectMode.READONLY)));
    }

    @Test
    void shouldGetEmptyWhenSubjectHasNoMode() throws Exception {
        // Given:
        when(client.getMode(SUBJECT)).thenThrow(new RestClientException("not found", 404, 40401));

        // When:
        final Optional<SubjectMode> mode = modes.get(SUBJECT);

        // Then:
        assertThat(mode, is(Optional.empty()));
    }

    @Test
    void shouldWrapRegistryErrorWhenGettingMode() throws Exception {
        // Given:
        final RestClientException error = new RestClientException("forbidden", 403, 40301);
        when(client.getMode(SUBJECT)).thenThrow(error);

        // When:
        final SubjectModeException thrown =
                assertThrows(SubjectModeException.class, () -> modes.get(SUBJECT));

        // Then:
        assertThat(thrown.getMessage(), is("Failed to get mode for subject: " + SUBJECT));
        assertThat(thrown.getCause(), is(sameInstance(error)));
    }

    @Test
    void shouldWrapUnknownMode() throws Exception {
        // Given:
        when(client.getMode(SUBJECT)).thenReturn("SOMETHING_NEW");

        // When:
        final SubjectModeException thrown =
                assertThrows(SubjectModeException.class, () -> modes.get(SUBJECT));

        // Then:
        assertThat(thrown.getMessage(), is("Unknown mode 'SOMETHING_NEW' for subject: " + SUBJECT));
    }

    @Test
    void shouldSetSubjectMode() throws Exception {
        // When:
        modes.set(SUBJECT, SubjectMode.READONLY);

        // Then:
        verify(client).setMode("READONLY", SUBJECT);
    }

    @Test
    void shouldWrapRegistryErrorWhenSettingMode() throws Exception {
        // Given:
        final RestClientException error = new RestClientException("forbidden", 403, 40301);
        when(client.setMode("READONLY", SUBJECT)).thenThrow(error);

        // When:
        final SubjectModeException thrown =
                assertThrows(
                        SubjectModeException.class, () -> modes.set(SUBJECT, SubjectMode.READONLY));

        // Then:
        assertThat(
                thrown.getMessage(),
                startsWith("Failed to set mode READONLY for subject: " + SUBJECT));
        assertThat(thrown.getMessage(), containsString("mode.mutability"));
        assertThat(thrown.getCause(), is(sameInstance(error)));
    }

    @Test
    void shouldRemoveSubjectMode() throws Exception {
        // When:
        modes.remove(SUBJECT);

        // Then:
        verify(client).deleteMode(SUBJECT);
    }

    @Test
    void shouldWrapRegistryErrorWhenRemovingMode() throws Exception {
        // Given:
        final RestClientException error = new RestClientException("forbidden", 403, 40301);
        doThrow(error).when(client).deleteMode(SUBJECT);

        // When:
        final SubjectModeException thrown =
                assertThrows(SubjectModeException.class, () -> modes.remove(SUBJECT));

        // Then:
        assertThat(
                thrown.getMessage(), startsWith("Failed to remove mode for subject: " + SUBJECT));
        assertThat(thrown.getMessage(), containsString("mode.mutability"));
        assertThat(thrown.getCause(), is(sameInstance(error)));
    }
}
