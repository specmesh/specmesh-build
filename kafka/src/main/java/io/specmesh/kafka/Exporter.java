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

import io.specmesh.apiparser.model.ApiSpec;
import io.specmesh.apiparser.model.Bindings;
import io.specmesh.apiparser.model.Channel;
import io.specmesh.apiparser.model.KafkaBinding;
import io.specmesh.apiparser.model.Operation;
import io.specmesh.kafka.provision.TopicProvisioner.Topic;
import io.specmesh.kafka.provision.TopicReaders;
import io.specmesh.kafka.provision.TopicReaders.TopicsReaderBuilder;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;

/**
 * Builds a simplified topic resources structure as the async-api spec limitations: atm only exports
 * domain owned topics, ignored producer and consumer models, ACLs, and Schemas
 */
public final class Exporter {

    /** Hides it */
    private Exporter() {}

    /**
     * Interrogate a cluster and extract domain-owned/aggregate resourcews
     *
     * @param aggregateId - the domain-owner
     * @param adminClient - cluster connection
     * @return the exported spec
     * @throws ExporterException - when admin client fails
     */
    public static ApiSpec export(final String aggregateId, final Admin adminClient)
            throws ExporterException {
        return ApiSpec.builder()
                .id("urn:" + aggregateId)
                .version(java.time.LocalDate.now().toString())
                .asyncapi("2.5.0")
                .channels(channels(aggregateId.replace(":", "."), adminClient))
                .build();
    }

    private static Map<String, Channel> channels(final String aggregateId, final Admin adminClient)
            throws ExporterException {

        return reader(aggregateId, adminClient).readall().stream()
                .collect(
                        Collectors.toMap(
                                topic -> topic.name().substring(aggregateId.length() + 1),
                                Exporter::channel));
    }

    /**
     * Get a topic reader
     *
     * @param aggregateId - filter prefix
     * @param adminClient - cluster connection
     * @return topic reader
     */
    private static TopicReaders.TopicReader reader(
            final String aggregateId, final Admin adminClient) {
        return TopicsReaderBuilder.builder(adminClient, aggregateId).build();
    }

    /**
     * Extract the Channel
     *
     * @param topic - kafka topic config map
     * @return decorated channel
     */
    private static Channel channel(final Topic topic) {
        return Channel.builder()
                .bindings(Bindings.builder().kafka(kafkaBindings(topic)).build())
                .publish(Operation.builder().build())
                .build();
    }

    private static KafkaBinding kafkaBindings(final Topic topic) {
        return KafkaBinding.builder()
                .bindingVersion("unknown")
                .replicas(topic.replication())
                .partitions(topic.partitions())
                .configs(topic.config())
                .build();
    }

    /** Thrown when the admin client cannot interact with the cluster */
    public static class ExporterException extends RuntimeException {
        ExporterException(final String message, final Exception cause) {
            super(message, cause);
        }
    }
}
