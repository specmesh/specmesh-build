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

import static org.apache.kafka.common.config.TopicConfig.RETENTION_MS_CONFIG;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.specmesh.kafka.provision.Provisioner.ProvisioningException;
import io.specmesh.kafka.provision.Status.STATE;
import io.specmesh.kafka.provision.TopicProvisioner.Topic;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreatePartitionsOptions;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;

/** Write topics using provided input set */
public class TopicMutators {

    /** collection based */
    public static final class CollectiveMutator implements TopicMutator {

        private final Stream<TopicMutator> writers;

        /**
         * iterate over the writers
         *
         * @param writers to iterate
         */
        private CollectiveMutator(final TopicMutator... writers) {
            this.writers = Arrays.stream(writers);
        }

        /**
         * write topic updates
         *
         * @param topics to write
         * @return updated status
         */
        @Override
        public Collection<Topic> mutate(final Collection<Topic> topics) {
            return this.writers
                    .map(writer -> writer.mutate(topics))
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
        }
    }

    /** updates */
    public static final class UpdateMutator implements TopicMutator {

        private final Admin adminClient;

        /**
         * Needs the admin client
         *
         * @param adminClient - cluster connection
         */
        UpdateMutator(final Admin adminClient) {
            this.adminClient = adminClient;
        }

        /**
         * Write the given topics and update the status flag appropriately
         *
         * @param topics to write
         * @return topics with updated flag
         * @throws ProvisioningException when things break
         */
        public Collection<Topic> mutate(final Collection<Topic> topics)
                throws ProvisioningException {

            final var topicsToUpdate =
                    topics.stream()
                            .filter(topic -> topic.state().equals(STATE.UPDATE))
                            .collect(Collectors.toList());

            final var topicNames = toTopicNames(topicsToUpdate);
            final var describeTopics = adminClient.describeTopics(topicNames).topicNameValues();

            topicsToUpdate.forEach(
                    topic -> {
                        try {
                            final var description =
                                    describeTopics
                                            .get(topic.name())
                                            .get(Provisioner.REQUEST_TIMEOUT, TimeUnit.SECONDS);
                            updatePartitions(topic, description);
                            updateConfigs(topic);
                        } catch (InterruptedException | ExecutionException | TimeoutException ex) {
                            throw new ProvisioningException("Failed to update configs", ex);
                        }
                    });
            return topicsToUpdate;
        }

        /**
         * convert to names
         *
         * @param topicsToUpdate source list
         * @return just the names
         */
        private List<String> toTopicNames(final List<Topic> topicsToUpdate) {
            return topicsToUpdate.stream().map(Topic::name).collect(Collectors.toList());
        }

        /**
         * See <a
         * href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-339%3A+Create+a+new+IncrementalAlterConfigs+API">...</a>
         * for more details update topic.config retention without a change ij value is a noop
         *
         * @param topic to update
         */
        private void updateConfigs(final Topic topic) {

            try {
                final var alterConfigOps =
                        topic.config().entrySet().stream()
                                .map(
                                        entry ->
                                                new AlterConfigOp(
                                                        new ConfigEntry(
                                                                entry.getKey(), entry.getValue()),
                                                        AlterConfigOp.OpType.SET))
                                .collect(Collectors.toList());

                final Map<ConfigResource, Collection<AlterConfigOp>> configs =
                        Map.of(
                                new ConfigResource(ConfigResource.Type.TOPIC, topic.name()),
                                alterConfigOps);
                adminClient.incrementalAlterConfigs(
                        configs, new AlterConfigsOptions().timeoutMs(Provisioner.REQUEST_TIMEOUT));
                topic.messages(
                        topic.messages()
                                + "\nUpdated config: "
                                + RETENTION_MS_CONFIG
                                + " -> "
                                + topic.config().get(RETENTION_MS_CONFIG));
                topic.state(STATE.UPDATED);

            } catch (Exception ex) {
                topic.state(STATE.FAILED)
                        .exception(new ProvisioningException("Failed to update config ", ex));
            }
        }

        private void updatePartitions(final Topic topic, final TopicDescription description) {

            try {
                if (description.partitions().size() < topic.partitions()) {

                    final var parts =
                            adminClient.createPartitions(
                                    Map.of(
                                            topic.name(),
                                            NewPartitions.increaseTo(topic.partitions())),
                                    new CreatePartitionsOptions().retryOnQuotaViolation(false));

                    parts.all().get(Provisioner.REQUEST_TIMEOUT, TimeUnit.SECONDS);
                    topic.state(STATE.UPDATED);
                    topic.messages(topic.messages() + "\n" + " Updated partitionCount");
                } else {
                    topic.messages(
                            topic.messages()
                                    + "\n"
                                    + " Ignoring partition increase because new count is not"
                                    + " higher");
                }
            } catch (Exception ex) {
                topic.state(STATE.FAILED)
                        .exception(new ProvisioningException("Failed to update partitions", ex));
            }
        }
    }

    /** delete non-spec resources */
    public static final class CleanUnspecifiedMutator implements TopicMutator {

        private final boolean dryRun;
        private final Admin adminClient;

        /**
         * Needs the admin client
         *
         * @param dryRun - test or execute flag
         * @param adminClient - cluster connection
         */
        CleanUnspecifiedMutator(final boolean dryRun, final Admin adminClient) {
            this.dryRun = dryRun;
            this.adminClient = adminClient;
        }

        /**
         * Remove topics that are not CREATE or UPSDATE (i.e. not in the spec)
         *
         * @param topics to write
         * @return topics with updated flag
         * @throws ProvisioningException when things break
         */
        public Collection<Topic> mutate(final Collection<Topic> topics)
                throws ProvisioningException {

            // spec topics will have CREATE or UPDATE status - remove the others (unwanted)
            final var unwanted =
                    topics.stream()
                            .filter(
                                    topic ->
                                            !topic.state().equals(STATE.CREATE)
                                                    && !topic.state().equals(STATE.UPDATE))
                            .collect(Collectors.toList());

            try {
                if (!dryRun) {
                    adminClient
                            .deleteTopics(toTopicNames(unwanted))
                            .all()
                            .get(Provisioner.REQUEST_TIMEOUT, TimeUnit.SECONDS);
                }
            } catch (InterruptedException | ExecutionException | TimeoutException ex) {
                throw new ProvisioningException("Failed to cleanup unwanted topics", ex);
            }
            return unwanted;
        }

        /**
         * convert to names
         *
         * @param topicsToUpdate source list
         * @return just the names
         */
        private List<String> toTopicNames(final List<Topic> topicsToUpdate) {
            return topicsToUpdate.stream().map(Topic::name).collect(Collectors.toList());
        }
    }

    /** creations */
    public static final class CreateMutator implements TopicMutator {

        private final Admin adminClient;

        /**
         * Needs the admin client
         *
         * @param adminClient - cluster connection
         */
        private CreateMutator(final Admin adminClient) {
            this.adminClient = adminClient;
        }

        /**
         * Write the given topics and update the status flag appropriately
         *
         * @param topics to write
         * @return topics with updated flag
         * @throws ProvisioningException when things break
         */
        public Collection<Topic> mutate(final Collection<Topic> topics)
                throws ProvisioningException {

            final var topicsToCreate =
                    topics.stream()
                            .filter(topic -> topic.state().equals(STATE.CREATE))
                            .collect(Collectors.toList());
            try {
                adminClient
                        .createTopics(asNewTopic(topicsToCreate))
                        .all()
                        .get(Provisioner.REQUEST_TIMEOUT, TimeUnit.SECONDS);
                return topicsToCreate.stream()
                        .map(topic -> topic.state(STATE.CREATED))
                        .collect(Collectors.toList());
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                topicsToCreate.forEach(
                        topic ->
                                topic.exception(
                                                new ProvisioningException(
                                                        "failed to write topics", e))
                                        .state(STATE.FAILED));
            }
            return topics;
        }

        /**
         * Convert to appropriate Kafka request type
         *
         * @param topics to create from
         * @return kafka type object
         */
        private Collection<NewTopic> asNewTopic(final Collection<Topic> topics) {
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

    /** Noopper that does nada */
    public static final class NoopMutator implements TopicMutator {
        /**
         * Do nothing write
         *
         * @param topics to ignore
         * @return unmodified list
         * @throws ProvisioningException when things go wrong
         */
        public Collection<Topic> mutate(final Collection<Topic> topics)
                throws ProvisioningException {
            return topics;
        }
    }

    /** Interface for writing/mutating topics to kafka */
    interface TopicMutator {
        /**
         * Api for writing
         *
         * @param topics to do stuff against
         * @return updated state of topics written
         */
        Collection<Topic> mutate(Collection<Topic> topics);
    }

    /** TopicMutator builder */
    @SuppressFBWarnings(
            value = "EI_EXPOSE_REP2",
            justification = "adminClient() passed as param to prevent API pollution")
    public static final class TopicMutatorBuilder {
        private Admin adminClient;
        private boolean noop;
        private boolean cleanUnspecified;
        private boolean dryRun;

        /** defensive */
        private TopicMutatorBuilder() {}

        /**
         * add the adminClient
         *
         * @param adminClient - cluster connection
         * @return builder
         */
        public TopicMutatorBuilder adminClient(final Admin adminClient) {
            this.adminClient = adminClient;
            return this;
        }

        /**
         * use a noop mutator
         *
         * @param dryRun - true is dry running
         * @return the builder
         */
        public TopicMutatorBuilder noopMutator(final boolean dryRun) {
            this.noop = dryRun;
            return this;
        }
        /**
         * use the delete mutator
         *
         * @param cleanUnspecified - to cleanup resources
         * @param dryRun - test.validate the proposed operation
         * @return the builder
         */
        public TopicMutatorBuilder cleanUnspecified(
                final boolean cleanUnspecified, final boolean dryRun) {
            this.cleanUnspecified = cleanUnspecified;
            this.dryRun = dryRun;
            return this;
        }

        /**
         * main builder
         *
         * @return builder
         */
        public static TopicMutatorBuilder builder() {
            return new TopicMutatorBuilder();
        }

        /**
         * build it
         *
         * @return the specified topic mutator impl
         */
        public TopicMutator build() {
            if (cleanUnspecified) {
                return new CleanUnspecifiedMutator(dryRun, adminClient);
            } else if (noop) {
                return new NoopMutator();
            } else {
                return new CollectiveMutator(
                        new CreateMutator(adminClient), new UpdateMutator(adminClient));
            }
        }
    }
}
