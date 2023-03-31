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
import io.specmesh.kafka.KafkaApiSpec;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.admin.Admin;

/** Provisions topics */
public final class TopicProvisioner {

    private TopicProvisioner() {}

    /**
     * Provision topics in the Kafka cluster.
     *
     * @param dryRun test or execute
     * @param apiSpec the api spec.
     * @param adminClient admin client for the Kafka cluster.
     * @return number of topics created
     * @throws Provisioner.ProvisioningException on provision failure
     */
    public static Collection<Topic> provision(
            final boolean dryRun, final KafkaApiSpec apiSpec, final Admin adminClient) {
        final var domain = domainTopicsFromApiSpec(apiSpec);
        final var existing = reader(apiSpec, adminClient).readall();
        final var changeSet = comparator().calculate(existing, domain);
        return writer(dryRun, adminClient).write(changeSet);
    }

    /**
     * gets the comparator
     *
     * @return comparator
     */
    private static TopicChangeSetCalculators.ChangeSetCalculator comparator() {
        return TopicChangeSetCalculators.ChangeSetBuilder.builder().build();
    }

    /**
     * Gets a writer
     *
     * @param dryRun to ignore writing to the cluster
     * @param adminClient - cluster connection
     * @return configured writer
     */
    private static TopicWriters.TopicWriter writer(final boolean dryRun, final Admin adminClient) {
        final var topicWriterBuilder = TopicWriters.TopicWriterBuilder.builder().noopWriter(dryRun);
        return topicWriterBuilder.adminClient(adminClient).build();
    }

    /**
     * Reads from the cluster
     *
     * @param apiSpec - to use for selecting topics
     * @param adminClient - cluster connection
     * @return reader
     */
    private static TopicReaders.TopicReader reader(
            final KafkaApiSpec apiSpec, final Admin adminClient) {
        return TopicReaders.TopicsReaderBuilder.builder(adminClient, apiSpec.id()).build();
    }

    /**
     * Topics from the api spec
     *
     * @param apiSpec - spec
     * @return set of topics from the spec
     */
    private static Collection<Topic> domainTopicsFromApiSpec(final KafkaApiSpec apiSpec) {
        return apiSpec.listDomainOwnedTopics().stream()
                .map(
                        newTopic ->
                                Topic.builder()
                                        .name(newTopic.name())
                                        .state(Status.STATE.READ)
                                        .partitions(newTopic.numPartitions())
                                        .replication(newTopic.replicationFactor())
                                        .config(newTopic.configs())
                                        .build())
                .collect(Collectors.toList());
    }

    /** Topic provisioning status */
    @Builder
    @Data
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    @Accessors(fluent = true)
    @SuppressFBWarnings
    public static class Topic {
        private String name;
        private Status.STATE state;
        private int partitions;
        private short replication;
        private Map<String, String> config;
        private Exception exception;
        private String messages;
    }
}
