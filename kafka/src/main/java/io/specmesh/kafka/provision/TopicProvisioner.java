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

import com.google.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.specmesh.kafka.KafkaApiSpec;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
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
     * @param cleanUnspecified remove unwanted resources
     * @param partitionCountFactor Optional factor used to scale the number of partitions used when
     *     provisioning a topic. Must be 0 < factor <= 1.0. For example, if factor is 0.5, then all
     *     topics will be created with half the number of partitions specified in the spec. Topics
     *     with more than 2 or more partition in the spec will always have at least 2 partitions
     *     after scaling.
     * @param apiSpec the api spec.
     * @param adminClient admin client for the Kafka cluster.
     * @return number of topics created
     * @throws ProvisioningException on provision failure
     */
    public static Collection<Topic> provision(
            final boolean dryRun,
            final boolean cleanUnspecified,
            final double partitionCountFactor,
            final KafkaApiSpec apiSpec,
            final Admin adminClient) {
        final var domain = domainTopicsFromApiSpec(apiSpec, partitionCountFactor);
        final var existing = reader(apiSpec, adminClient).readall();
        final var changeSet = comparator(cleanUnspecified).calculate(existing, domain);
        return mutate(dryRun, cleanUnspecified, adminClient).mutate(changeSet);
    }

    /**
     * gets the comparator
     *
     * @return comparator
     */
    private static TopicChangeSetCalculators.ChangeSetCalculator comparator(
            final boolean cleanUnspecified) {
        return TopicChangeSetCalculators.ChangeSetBuilder.builder().build(cleanUnspecified);
    }

    /**
     * Gets a writer
     *
     * @param dryRun - to ignore writing to the cluster
     * @param cleanupUnspecified - remove set of unspec'd resources
     * @param adminClient - cluster connection
     * @return configured writer
     */
    private static TopicMutators.TopicMutator mutate(
            final boolean dryRun, final boolean cleanupUnspecified, final Admin adminClient) {
        final var topicWriterBuilder =
                TopicMutators.TopicMutatorBuilder.builder()
                        .noopMutator(dryRun)
                        .cleanUnspecified(cleanupUnspecified, dryRun);
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

    private static Collection<Topic> domainTopicsFromApiSpec(
            final KafkaApiSpec apiSpec, final double partitionCountFactor) {
        return apiSpec.listDomainOwnedTopics().stream()
                .map(
                        newTopic ->
                                Topic.builder()
                                        .name(newTopic.name())
                                        .state(Status.STATE.READ)
                                        .partitions(
                                                topicPartitions(
                                                        newTopic.numPartitions(),
                                                        partitionCountFactor))
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
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    public static final class Topic implements ProvisioningTask {
        @EqualsAndHashCode.Include private String name;
        private Status.STATE state;
        private int partitions;
        private short replication;
        @Builder.Default private Map<String, String> config = Map.of();
        private Exception exception;
        @Builder.Default private String messages = "";

        @Override
        public String id() {
            return "Topic:" + name;
        }

        public Topic exception(final Exception exception) {
            this.exception = new ExceptionWrapper(exception);
            this.state = Status.STATE.FAILED;
            return this;
        }
    }

    private static int topicPartitions(
            final int specPartitionCount, final double partitionCountFactor) {
        Preconditions.checkArgument(
                0.0 < partitionCountFactor && partitionCountFactor <= 1.0,
                "0.0 < partitionCountFactor <= 1.0, but was: " + partitionCountFactor);
        final int min = specPartitionCount < 2 ? 1 : 2;
        return Math.max(min, (int) (specPartitionCount * partitionCountFactor));
    }
}
