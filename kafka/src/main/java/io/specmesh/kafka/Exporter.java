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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.specmesh.apiparser.model.ApiSpec;
import io.specmesh.apiparser.model.Bindings;
import io.specmesh.apiparser.model.Channel;
import io.specmesh.apiparser.model.KafkaBinding;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;

/**
 * Builds a simplified topic resources structure as the async-api spec limitations: atm only exports
 * domain owned topics, ignored producer and consumer models, ACLs, and Schemas
 */
public class Exporter {

    /**
     * Export the Spec object to its yaml representation
     *
     * @param exported - the hydrated spec to convert to yaml
     * @return the asyncapi spec
     * @throws ExporterException - when json cannot be handled
     */
    public static String exportYaml(final ApiSpec exported) throws ExporterException {
        try {
            return new ObjectMapper(new YAMLFactory()).writeValueAsString(exported);
        } catch (JsonProcessingException e) {
            throw new ExporterException("Failed to convert to YAML", e);
        }
    }

    /**
     * Integrrogate a cluster and extract domain-owned/aggregate resourcews
     *
     * @param aggregateId - the domain-owner
     * @param adminClient - cluster connection
     * @return the exported spec
     * @throws ExporterException - when admin client fails
     */
    public ApiSpec export(final String aggregateId, final Admin adminClient)
            throws ExporterException {
        return ApiSpec.builder()
                .id("urn:" + aggregateId)
                .version(java.time.LocalDate.now().toString())
                .asyncapi("2.5.0")
                .channels(channels(aggregateId.replace(":", "."), adminClient))
                .build();
    }

    private Map<String, Channel> channels(final String aggregateId, final Admin adminClient)
            throws ExporterException {
        final List<TopicListing> topicListings;
        try {
            topicListings =
                    adminClient.listTopics().listings().get().stream()
                            .filter(
                                    listing ->
                                            !listing.isInternal()
                                                    && listing.name().startsWith(aggregateId))
                            .collect(Collectors.toList());
        } catch (InterruptedException | ExecutionException e) {
            throw new ExporterException("Failed to list topics", e);
        }
        final Map<String, Config> topicConfigs;
        try {
            topicConfigs =
                    adminClient
                            .describeConfigs(
                                    topicListings.stream()
                                            .map(
                                                    item ->
                                                            new ConfigResource(
                                                                    ConfigResource.Type.TOPIC,
                                                                    item.name()))
                                            .collect(Collectors.toList()))
                            .all()
                            .get()
                            .entrySet()
                            .stream()
                            .collect(
                                    Collectors.toMap(
                                            entry -> entry.getKey().name(), Map.Entry::getValue));
        } catch (InterruptedException | ExecutionException e) {
            throw new ExporterException("Failed to describe topic configs", e);
        }

        final Map<String, KafkaFuture<TopicDescription>> topicDescriptions =
                adminClient
                        .describeTopics(
                                topicListings.stream()
                                        .map(TopicListing::name)
                                        .collect(Collectors.toList()))
                        .topicNameValues();

        return topicListings.stream()
                .collect(
                        Collectors.toMap(
                                listing -> listing.name().substring(aggregateId.length() + 1),
                                listing -> {
                                    try {
                                        return channel(
                                                topicConfigs.get(listing.name()),
                                                topicDescriptions.get(listing.name()).get());
                                    } catch (InterruptedException | ExecutionException e) {
                                        return Channel.builder().build();
                                    }
                                }));
    }

    /**
     * Extract the Channel - todo Produce/Consume info
     *
     * @param config
     * @param topicDescription
     * @return
     */
    private Channel channel(final Config config, final TopicDescription topicDescription) {
        return Channel.builder()
                .bindings(Bindings.builder().kafka(kafkaBindings(config, topicDescription)).build())
                .build();
    }

    private KafkaBinding kafkaBindings(
            final Config config, final TopicDescription topicDescription) {
        return KafkaBinding.builder()
                .bindingVersion("unknown")
                .replicas(topicDescription.partitions().get(0).replicas().size())
                .partitions(topicDescription.partitions().size())
                .retention(inferRetentionDays(config.get("retention.ms")))
                .configs(configs(config))
                .build();
    }

    private int inferRetentionDays(final ConfigEntry retention) {
        try {
            return Long.valueOf(Long.parseLong(retention.value()) / (1000L * 60 * 60 * 24))
                    .intValue();
        } catch (Exception exception) {
            exception.printStackTrace();
            return 1;
        }
    }

    private Map<String, String> configs(final Config config) {
        return config.entries().stream()
                .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value));
    }

    /** Thrown when the admin client cannot interact with the cluster */
    public static class ExporterException extends RuntimeException {
        ExporterException(final String message, final Exception cause) {
            super(message, cause);
        }
    }
}
