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

package io.specmesh.kafka.admin;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

/** Simple client used to query storage and consumption values */
public class SimpleAdminClient implements SmAdminClient {

    public static final long TIMEOUT = 300L;
    private final Admin adminClient;

    SimpleAdminClient(final Admin adminClient) {
        this.adminClient = adminClient;
    }

    /**
     * Get the groups for a topic prefix and their partition offsets
     *
     * @param topicPrefix to match against
     * @return the list of groups
     */
    @Override
    public List<ConsumerGroup> groupsForTopicPrefix(final String topicPrefix) {
        try {
            final var consumerGroups =
                    adminClient.listConsumerGroups().all().get(TIMEOUT, TimeUnit.SECONDS);
            final var groupDescriptions = groupsFor(topicPrefix, consumerGroups);

            return groupDescriptions.stream()
                    .map(
                            groupDescription -> {
                                final var groupBuilder = ConsumerGroup.builder();
                                groupBuilder.id(groupDescription.groupId());
                                groupBuilder.members(
                                        groupDescription.members().stream()
                                                .map(
                                                        member ->
                                                                Member.builder()
                                                                        .id(member.consumerId())
                                                                        .partitions(
                                                                                partitions(member))
                                                                        .host(member.host())
                                                                        .clientId(member.clientId())
                                                                        .build())
                                                .collect(Collectors.toList()));
                                final var group = groupBuilder.build();
                                group.calculateTotalOffset();
                                return group;
                            })
                    .collect(Collectors.toList());

        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new ClientException("Failed to list consumer-groups for:" + topicPrefix, e);
        }
    }

    /**
     * Set of groups for a prefix
     *
     * @param topicPrefix to match against
     * @param consumerGroups to filter from
     * @return matched descriptions
     */
    private List<ConsumerGroupDescription> groupsFor(
            final String topicPrefix, final Collection<ConsumerGroupListing> consumerGroups) {
        return consumerGroups.stream()
                .filter(
                        listing ->
                                !listing.isSimpleConsumerGroup()
                                        && listing.state().isPresent()
                                        && listing.state().get().equals(ConsumerGroupState.STABLE))
                .map(
                        group -> {
                            try {
                                return adminClient
                                        .describeConsumerGroups(List.of(group.groupId()))
                                        .all()
                                        .get()
                                        .values()
                                        .iterator()
                                        .next();
                            } catch (InterruptedException | ExecutionException e) {
                                throw new RuntimeException(e);
                            }
                        })
                .filter(
                        description ->
                                !description.members().isEmpty()
                                        && isConsumingFromTopicPrefix(description, topicPrefix))
                .collect(Collectors.toList());
    }

    /**
     * Build partition info for this member
     *
     * @param member to collect info for
     * @return set of partition data
     */
    private List<Partition> partitions(final MemberDescription member) {
        try {
            final var partitions =
                    adminClient
                            .listOffsets(
                                    member.assignment().topicPartitions().stream()
                                            .collect(
                                                    Collectors.toMap(
                                                            tp -> tp, tp -> OffsetSpec.latest())))
                            .all()
                            .get(TIMEOUT, TimeUnit.SECONDS);
            return partitions.entrySet().stream()
                    .map(
                            entry ->
                                    Partition.builder()
                                            .id(entry.getKey().partition())
                                            .topic(entry.getKey().topic())
                                            .offset(entry.getValue().offset())
                                            .timestamp(entry.getValue().timestamp())
                                            .build())
                    .collect(Collectors.toList());
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new ClientException("Failed to list partitions for:" + member, e);
        }
    }

    /**
     * Check if a cgroup is consuming from a topic prefix
     *
     * @param groupDescription - group desc
     * @param prefix to match againt
     * @return true when it is
     */
    private boolean isConsumingFromTopicPrefix(
            final ConsumerGroupDescription groupDescription, final String prefix) {
        return groupDescription
                .members()
                .iterator()
                .next()
                .assignment()
                .topicPartitions()
                .iterator()
                .next()
                .topic()
                .startsWith(prefix);
    }

    /**
     * Retrieve topic volume in bytes
     *
     * @param topic to query
     * @return total volume (including replica)
     */
    @Override
    public long topicVolumeUsingLogDirs(final String topic) {
        try {
            final var brokers = brokerIds();
            final var logDirsResult = adminClient.describeLogDirs(brokers);
            final var logDirsByBroker =
                    logDirsResult.allDescriptions().get(TIMEOUT, TimeUnit.SECONDS);

            long totalSize = 0;

            for (final var logDirs : logDirsByBroker.values()) {
                for (final var logDirInfo : logDirs.values()) {
                    for (final var replicaInfoEntry : logDirInfo.replicaInfos().entrySet()) {
                        final var tp = replicaInfoEntry.getKey();
                        if (topic.equals(tp.topic())) {
                            totalSize += replicaInfoEntry.getValue().size();
                        }
                    }
                }
            }

            return totalSize;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new ClientException("Failed to get topicVolumeUsingLogDirs:" + topic, e);
        }
    }

    /**
     * List all brokerIds
     *
     * @return the set of brokerIds
     */
    @Override
    public List<Integer> brokerIds() {
        try {
            return adminClient.describeCluster().nodes().get().stream()
                    .mapToInt(Node::id)
                    .boxed()
                    .collect(Collectors.toList());
        } catch (InterruptedException | ExecutionException e) {
            throw new ClientException("Failed to describe cluster to get brokerIds", e);
        }
    }

    /**
     * Get topic offset total
     *
     * @param topic to query
     * @return total offset count
     */
    @Override
    public long topicVolumeOffsets(final String topic) {

        try {
            final var describeTopicsResult =
                    adminClient.describeTopics(Collections.singleton(topic));
            final var topicDescription =
                    describeTopicsResult.allTopicNames().get(TIMEOUT, TimeUnit.SECONDS).get(topic);

            // Get the start and end offsets for each partition
            final Map<TopicPartition, OffsetSpec> endOffsetsQuery = new HashMap<>();
            final Map<TopicPartition, OffsetSpec> startOffsetsQuery = new HashMap<>();
            for (TopicPartitionInfo partitionInfo : topicDescription.partitions()) {
                final var partition = new TopicPartition(topic, partitionInfo.partition());
                endOffsetsQuery.put(partition, OffsetSpec.latest());
                startOffsetsQuery.put(partition, OffsetSpec.earliest());
            }

            final var endOffsetsResult = adminClient.listOffsets(endOffsetsQuery);
            final var startOffsetsResult = adminClient.listOffsets(startOffsetsQuery);

            long totalVolume = 0;
            for (TopicPartition partition : endOffsetsQuery.keySet()) {
                final long endOffset = endOffsetsResult.partitionResult(partition).get().offset();
                final long startOffset =
                        startOffsetsResult.partitionResult(partition).get().offset();
                totalVolume += endOffset - startOffset;
            }

            return totalVolume;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new ClientException("Failed to get topicVolumeOffsets for topic:" + topic, e);
        }
    }
}
