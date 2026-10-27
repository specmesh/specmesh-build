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

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.Optional;

/** Reads and changes the modes of Schema Registry subjects. */
final class SubjectModes {

    /** Explains the usual causes of a failed mode change. */
    static final String MODE_CHANGE_HINT =
            "Ensure the Schema Registry allows mode changes (mode.mutability=true) and the"
                    + " principal can change subject modes.";

    private static final int NOT_FOUND = 404;

    private final SchemaRegistryClient client;

    SubjectModes(final SchemaRegistryClient client) {
        this.client = requireNonNull(client, "client");
    }

    /**
     * @param subject the subject
     * @return the mode set on the subject itself, or empty if it has none.
     * @throws IllegalArgumentException if the registry returns an unknown mode.
     */
    Optional<SubjectMode> get(final String subject) throws IOException, RestClientException {
        try {
            return Optional.ofNullable(client.getMode(subject)).map(SubjectMode::valueOf);
        } catch (RestClientException e) {
            if (e.getStatus() == NOT_FOUND) {
                return Optional.empty();
            }
            throw e;
        }
    }

    /**
     * @param subject the subject
     * @param mode the mode to set on the subject
     */
    void set(final String subject, final SubjectMode mode) throws IOException, RestClientException {
        client.setMode(mode.name(), subject);
    }

    /**
     * Remove the subject's own mode, so it falls back to the global mode.
     *
     * @param subject the subject
     */
    void remove(final String subject) throws IOException, RestClientException {
        client.deleteMode(subject);
    }
}
