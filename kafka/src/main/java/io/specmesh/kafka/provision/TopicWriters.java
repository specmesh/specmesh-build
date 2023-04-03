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
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;

/** Write topics using provided input set */
public class TopicWriters {

    /** Does real topic writing */
    public static final class RealTopicWriter implements TopicWriter {

        private final Admin adminClient;

        /**
         * Needs the admin client
         *
         * @param adminClient - cluster connection
         */
        private RealTopicWriter(final Admin adminClient) {
            this.adminClient = adminClient;
        }

        /**
         * Write the given topics and update the status flag appropriately
         *
         * @param topics to write
         * @return topics with updated flag
         * @throws Provisioner.ProvisioningException when things break
         */
        public Collection<TopicProvisioner.Topic> write(
                final Collection<TopicProvisioner.Topic> topics)
                throws Provisioner.ProvisioningException {

            final var topicsToCreate =
                    topics.stream()
                            .filter(topic -> topic.state().equals(Status.STATE.CREATE))
                            .collect(Collectors.toList());
            try {
                adminClient
                        .createTopics(asNewTopic(topicsToCreate))
                        .all()
                        .get(Provisioner.REQUEST_TIMEOUT, TimeUnit.SECONDS);
                return topics.stream()
                        .map(topic -> topic.state(Status.STATE.CREATED))
                        .collect(Collectors.toList());
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                topicsToCreate
                        .forEach(
                                topic ->
                                        topic.exception(
                                                        new Provisioner.ProvisioningException(
                                                                "failed to write topics", e))
                                                .state(Status.STATE.FAILED));
            }
            return topics;
        }

        /**
         * Convert to appropriate Kafka request type
         *
         * @param topics to create from
         * @return kafka type object
         */
        private Collection<NewTopic> asNewTopic(final Collection<TopicProvisioner.Topic> topics) {
            return topics.stream()
                    .map(
                            topic ->
                                    new NewTopic(
                                                    topic.name(),
                                                    topic.partitions(),
                                                    topic.replication())
                                            .configs(topic.config()))
                    .collect(Collectors.toList());
        }
    }

    /** Noop write that does nada */
    public static final class NoopTopicWriter implements TopicWriter {
        /**
         * Do nothing write
         *
         * @param topics to ignore
         * @return upmodified list
         * @throws Provisioner.ProvisioningException when things go wrong
         */
        public Collection<TopicProvisioner.Topic> write(
                final Collection<TopicProvisioner.Topic> topics)
                throws Provisioner.ProvisioningException {
            return topics;
        }
    }

    /** Interfacer for writing topics to kafka */
    interface TopicWriter {
        /**
         * Api for writing
         *
         * @param topics to write
         * @return updated state of topics written
         */
        Collection<TopicProvisioner.Topic> write(Collection<TopicProvisioner.Topic> topics);
    }

    /** TopicWriter builder */
    @SuppressFBWarnings(
            value = "EI_EXPOSE_REP2",
            justification = "adminClient() passed as param to prevent API pollution")
    public static final class TopicWriterBuilder {
        private Admin adminClient;
        private boolean noopWriter;

        /** defensive */
        private TopicWriterBuilder() {}

        /**
         * add the adminClient
         *
         * @param adminClient - cluster connection
         * @return builder
         */
        public TopicWriterBuilder adminClient(final Admin adminClient) {
            this.adminClient = adminClient;
            return this;
        }

        /**
         * use a noop writer
         *
         * @param dryRun - true is dry running
         * @return the builder
         */
        public TopicWriterBuilder noopWriter(final boolean dryRun) {
            this.noopWriter = dryRun;
            return this;
        }

        /**
         * main builder
         *
         * @return builder
         */
        public static TopicWriterBuilder builder() {
            return new TopicWriterBuilder();
        }

        /**
         * build it
         *
         * @return the specified topic writer impl
         */
        public TopicWriter build() {
            if (noopWriter) {
                return new NoopTopicWriter();
            } else {
                return new RealTopicWriter(adminClient);
            }
        }
    }
}
