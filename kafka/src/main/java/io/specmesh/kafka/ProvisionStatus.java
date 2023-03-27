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

package io.specmesh.kafka;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.specmesh.apiparser.model.SchemaInfo;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.resource.ResourcePattern;

/** Accumulated Provision status of Underlying resources */
@Builder
@Data
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Accessors(fluent = true)
@SuppressFBWarnings
public class ProvisionStatus {
    private Topics topics;
    private Schemas schemas;
    private Acls acls;

    /** Process the accumulated state */
    public void build() {
        topics.build();
        schemas.build();
        acls.build();
    }

    /** Topic provisioning status set */
    @Builder
    @Data
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    @Accessors(fluent = true)
    @SuppressFBWarnings
    public static class Topics {

        @Builder.Default private List<NewTopic> domainTopics = Collections.emptyList();
        @Builder.Default private List<String> existingTopics = Collections.emptyList();
        @Builder.Default private List<NewTopic> createTopics = Collections.emptyList();

        @Builder.Default private List<NewTopic> createdTopics = Collections.emptyList();

        private Exception exception;
        private Map<String, TopicStatus> status;

        /** Process the accumulated state */
        public void build() {
            status =
                    domainTopics.stream()
                            .map(
                                    domainTopic -> {
                                        final var status =
                                                TopicStatus.builder().name(domainTopic.name());
                                        final var shouldCreate =
                                                createTopics.stream()
                                                        .filter(
                                                                createTopic ->
                                                                        createTopic
                                                                                .name()
                                                                                .equals(
                                                                                        domainTopic
                                                                                                .name()))
                                                        .findFirst();
                                        final var wasCreated =
                                                createdTopics.stream()
                                                        .filter(
                                                                createdTopic ->
                                                                        createdTopic
                                                                                .name()
                                                                                .equals(
                                                                                        domainTopic
                                                                                                .name()))
                                                        .findFirst();

                                        // is there an action to carry out
                                        var createdStatus =
                                                shouldCreate.isPresent()
                                                        ? CRUD.CREATE
                                                        : CRUD.IGNORED;
                                        // if not ignored - was it created or updated
                                        createdStatus =
                                                createdStatus != CRUD.IGNORED
                                                                && shouldCreate.isPresent()
                                                                && wasCreated.isPresent()
                                                        ? CRUD.CREATED
                                                        : CRUD.CREATE;

                                        status.crud(createdStatus)
                                                .configs(domainTopic.configs())
                                                .partitions(domainTopic.numPartitions())
                                                .replication(domainTopic.replicationFactor());
                                        if (exception != null) {
                                            status.crud(CRUD.FAILED);
                                            status.exception(exception);
                                        }
                                        return status.build();
                                    })
                            .collect(Collectors.toMap(TopicStatus::name, item -> item));
        }
    }
    /** Topic provisioning status */
    @Builder
    @Data
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    @Accessors(fluent = true)
    @SuppressFBWarnings
    public static class TopicStatus {
        private String name;
        private CRUD crud;
        private int partitions;
        private short replication;
        private Map<String, String> configs;
        private Exception exception;
    }

    /** Operation result */
    public enum CRUD {
        /**
         *  intention to create
         */
        CREATE,
        /**
         * successfully creates
         */

        CREATED,
        /**
         * intention to update
         */
        UPDATE,
        /**
         * updated successfully
         */
        UPDATED,
        /**
         * intention to delete
         */
         DELETE,
        /**
         * deleted successfully
         */
         DELETED,
        /**
         * Noop
         */
        IGNORED,
        /**
         * operation failed
         */
        FAILED
    }

    /** Schema provisioning status */
    @Builder
    @Data
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    @Accessors(fluent = true)
    @SuppressFBWarnings
    public static class Schemas {
        @Builder.Default private List<SchemaStatus> schemas = Collections.emptyList();
        private Map<String, SchemaStatus> status;

        /**
         * compile state collections into the validation state map
         */
        public void build() {
            status =
                    schemas.stream()
                            .collect(Collectors.toMap(SchemaStatus::schemaSubject, sss -> sss));
        }
    }

    /** Schema provisioning status */
    @Builder
    @Data
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    @Accessors(fluent = true)
    @SuppressFBWarnings
    public static class SchemaStatus {
        private CRUD crud;
        private int id;
        private String schemaSubject;
        private SchemaInfo schemaInfo;
        private String schemaPath;
        private Exception exception;
    }

    /** Acl Provisioning status */
    @Builder
    @Data
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    @Accessors(fluent = true)
    @SuppressFBWarnings
    public static class Acls {
        @Builder.Default private Set<AclBinding> aclsToCreate = Collections.emptySet();
        @Builder.Default private Set<AclBinding> aclsCreated = Collections.emptySet();
        private Map<String, AclStatus> status;
        private Exception exception;

        /**
         * compile state collections into the validation state map
         */
        public void build() {
            status =
                    aclsToCreate.stream()
                            .map(
                                    acl -> {
                                        final var aclStatus =
                                                AclStatus.builder()
                                                        .name(acl.toString())
                                                        .entry(acl.entry())
                                                        .pattern(acl.pattern())
                                                        .crud(CRUD.CREATE);

                                        final var wasCreated =
                                                aclsCreated.stream()
                                                        .filter(
                                                                createdAcl ->
                                                                        createdAcl
                                                                                .toString()
                                                                                .equals(
                                                                                        acl
                                                                                                .toString()))
                                                        .findFirst();
                                        if (wasCreated.isPresent()) {
                                            aclStatus.crud(CRUD.CREATED);
                                        }
                                        if (exception != null) {
                                            aclStatus.crud(CRUD.FAILED);
                                            aclStatus.exception(exception);
                                        }
                                        return aclStatus.build();
                                    })
                            .collect(Collectors.toMap(AclStatus::name, aclStatus -> aclStatus));
        }
    }

    /** Acl status */
    @Builder
    @Data
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    @Accessors(fluent = true)
    @SuppressFBWarnings
    public static class AclStatus {
        private CRUD crud;
        private String name;
        private ResourcePattern pattern;
        private AccessControlEntry entry;
        private Exception exception;
    }
}
