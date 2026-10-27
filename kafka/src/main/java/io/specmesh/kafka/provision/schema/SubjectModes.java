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

    private static final String MODE_CHANGE_HINT =
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
     * @throws SubjectModeException if the mode cannot be read or is unknown.
     */
    Optional<SubjectMode> get(final String subject) {
        final String mode;
        try {
            mode = client.getMode(subject);
        } catch (RestClientException e) {
            if (e.getStatus() == NOT_FOUND) {
                return Optional.empty();
            }
            throw new SubjectModeException("Failed to get mode for subject: " + subject, e);
        } catch (IOException e) {
            throw new SubjectModeException("Failed to get mode for subject: " + subject, e);
        }

        try {
            return Optional.ofNullable(mode).map(SubjectMode::valueOf);
        } catch (IllegalArgumentException e) {
            throw new SubjectModeException(
                    "Unknown mode '" + mode + "' for subject: " + subject, e);
        }
    }

    /**
     * @param subject the subject
     * @param mode the mode to set on the subject
     * @throws SubjectModeException if the mode cannot be set.
     */
    void set(final String subject, final SubjectMode mode) {
        try {
            client.setMode(mode.name(), subject);
        } catch (IOException | RestClientException e) {
            throw new SubjectModeException(
                    "Failed to set mode "
                            + mode
                            + " for subject: "
                            + subject
                            + ". "
                            + MODE_CHANGE_HINT,
                    e);
        }
    }

    /**
     * Remove the subject's own mode, so it falls back to the global mode.
     *
     * @param subject the subject
     * @throws SubjectModeException if the mode cannot be removed.
     */
    void remove(final String subject) {
        try {
            client.deleteMode(subject);
        } catch (IOException | RestClientException e) {
            throw new SubjectModeException(
                    "Failed to remove mode for subject: " + subject + ". " + MODE_CHANGE_HINT, e);
        }
    }

    /** A subject's mode could not be read or changed. */
    static final class SubjectModeException extends RuntimeException {

        SubjectModeException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }
}
