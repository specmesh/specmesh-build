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

import io.specmesh.kafka.provision.Provisioner.ProvisioningException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;

/** Reads topics from the cluster - only 1 implementation for this atm */
public final class TopicReaders {

    /** Simple impl to read topics for given prefix */
    public static final class SimpleTopicReader implements TopicReader {

        private final Admin adminClient;
        private final String prefix;

        /**
         * Main ctor
         *
         * @param adminClient for cluster connection
         * @param prefix to march against
         */
        private SimpleTopicReader(final Admin adminClient, final String prefix) {
            this.adminClient = adminClient;
            this.prefix = prefix;
        }

        /**
         * Read all topics from the cluster that match the prefix
         *
         * @return the matched topics
         */
        public Collection<TopicProvisioner.Topic> readall() {
            final var topicList = topicsForPrefix(adminClient, prefix);

            final var topicDescriptions = topicDescriptions(topicList);

            final var topicConfigs = topicConfigs(topicList);

            return topicList.stream()
                    .map(
                            topicName -> {
                                final var topicBuilder =
                                        TopicProvisioner.Topic.builder()
                                                .name(topicName)
                                                .state(Status.STATE.READ);
                                try {
                                    final var description = topicDescriptions.get(topicName).get();
                                    final var config =
                                            topicConfigs
                                                    .get(
                                                            new ConfigResource(
                                                                    ConfigResource.Type.TOPIC,
                                                                    topicName))
                                                    .entries()
                                                    .stream()
                                                    .collect(
                                                            Collectors.toMap(
                                                                    ConfigEntry::name,
                                                                    ConfigEntry::value));
                                    // could go wrong if reassignment is in progress
                                    final var replicationFactor =
                                            description
                                                    .partitions()
                                                    .iterator()
                                                    .next()
                                                    .replicas()
                                                    .size();

                                    topicBuilder
                                            .partitions(description.partitions().size())
                                            .replication((short) replicationFactor)
                                            .config(config);
                                } catch (Exception e) {
                                    topicBuilder.exception(e);
                                }
                                return topicBuilder.build();
                            })
                    .collect(Collectors.toList());
        }

        /**
         * Set of topic configs for the given list
         *
         * @param topicList to get configs for
         * @return topic configs
         * @throws ProvisioningException when cant retrieve configs
         */
        private Map<ConfigResource, Config> topicConfigs(final List<String> topicList)
                throws ProvisioningException {
            try {
                return adminClient
                        .describeConfigs(generateConfigs(topicList))
                        .all()
                        .get(Provisioner.REQUEST_TIMEOUT, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw new ProvisioningException("Failed to get topic configs", e);
            }
        }

        /**
         * get set of descriptions
         *
         * @param topicList to get descritions of
         * @return topic descriptions
         */
        private Map<String, KafkaFuture<TopicDescription>> topicDescriptions(
                final List<String> topicList) {
            return adminClient.describeTopics(topicList).topicNameValues();
        }

        /**
         * generate configs for request
         *
         * @param topicList to generate against
         * @return config resources request
         */
        private Set<ConfigResource> generateConfigs(final List<String> topicList) {
            return topicList.stream()
                    .map(topicName -> new ConfigResource(ConfigResource.Type.TOPIC, topicName))
                    .collect(Collectors.toSet());
        }

        /**
         * Read the set of topics that have this 'prefix'
         *
         * @param adminClient = cluster connection
         * @param prefix - filter against
         * @return the set of topics that match
         * @throws ProvisioningException when cluster connection failed
         */
        private static List<String> topicsForPrefix(final Admin adminClient, final String prefix) {
            try {
                return adminClient
                        .listTopics()
                        .listings()
                        .get(Provisioner.REQUEST_TIMEOUT, TimeUnit.SECONDS)
                        .stream()
                        .map(TopicListing::name)
                        .filter(name -> name.startsWith(prefix))
                        .collect(Collectors.toList());
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw new ProvisioningException("Failed to list topics", e);
            }
        }
    }

    /** Reader API */
    public interface TopicReader {
        Collection<TopicProvisioner.Topic> readall();
    }

    /** Builder for readers */
    public static final class TopicsReaderBuilder {
        private Admin adminClient;
        private String prefix;

        /** defensive */
        private TopicsReaderBuilder() {}

        /**
         * protected builder construction
         *
         * @param adminClient - cluster connection
         * @param prefix - matching filter
         * @return topic reader
         */
        public static TopicsReaderBuilder builder(final Admin adminClient, final String prefix) {
            return new TopicsReaderBuilder(adminClient, prefix);
        }

        /**
         * Main builder
         *
         * @param adminClient - required for default impl
         * @param prefix - required to filter topics against
         */
        private TopicsReaderBuilder(final Admin adminClient, final String prefix) {
            this.adminClient = adminClient;
            this.prefix = prefix;
        }

        /**
         * build it
         *
         * @return topic reader
         */
        public TopicReader build() {
            return new SimpleTopicReader(adminClient, prefix);
        }
    }
}
