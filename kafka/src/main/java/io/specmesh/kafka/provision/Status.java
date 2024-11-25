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

package io.specmesh.kafka.provision;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.specmesh.kafka.provision.schema.SchemaProvisioner;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/** Accumulated Provision status of Underlying resources */
@Builder
@Data
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Accessors(fluent = true)
@SuppressFBWarnings
public class Status {

    @Builder.Default private Collection<TopicProvisioner.Topic> topics = List.of();
    @Builder.Default private Collection<SchemaProvisioner.Schema> schemas = List.of();
    @Builder.Default private Collection<AclProvisioner.Acl> acls = List.of();

    /** Operation result */
    public enum STATE {
        /** represents READ state */
        READ,
        /** intention to create */
        CREATE,
        /** successfully creates */
        CREATED,
        /** intention to update */
        UPDATE,
        /** updated successfully */
        UPDATED,
        /** intention to delete */
        DELETE,
        /** deleted successfully */
        DELETED,
        /** Noop */
        IGNORED,
        /** operation failed */
        FAILED
    }

    /**
     * @return true if any operation failed.
     */
    public boolean failed() {
        return all().anyMatch(e -> e.state() == STATE.FAILED);
    }

    /** Throws exception if any errors occurred. */
    public void check() {
        final List<Exception> exceptions =
                all().flatMap(e -> Optional.ofNullable(e.exception()).stream())
                        .collect(Collectors.toList());

        if (!exceptions.isEmpty() || failed()) {
            throw new CompositeException(exceptions);
        }
    }

    private Stream<WithState> all() {
        return Stream.concat(Stream.concat(topics.stream(), schemas.stream()), acls.stream());
    }

    private static final class CompositeException extends RuntimeException {
        CompositeException(final List<Exception> exceptions) {
            super(
                    exceptions.stream()
                            .map(CompositeException::fullMessage)
                            .collect(Collectors.joining(System.lineSeparator())));
        }

        private static String fullMessage(final Exception e) {
            final StringWriter sw = new StringWriter();
            final PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            return e.getMessage() + System.lineSeparator() + sw;
        }
    }
}
