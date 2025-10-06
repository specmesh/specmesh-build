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

import io.specmesh.kafka.provision.TopicProvisioner.Topic;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Calculates a changeset of topics to create or update, should also return incompatible changes for
 * existing topics
 */
public class TopicChangeSetCalculators {

    /** Collection based one */
    public static final class CollectiveCalculator implements ChangeSetCalculator {

        private final Stream<ChangeSetCalculator> calculators;

        /**
         * Collection of calcs
         *
         * @param calculators to iterate over
         */
        private CollectiveCalculator(final ChangeSetCalculator... calculators) {
            this.calculators = Arrays.stream(calculators);
        }

        /**
         * Call all the calcs
         *
         * @param existingTopics - existing set of topics
         * @param requiredTopics - ownership topics
         * @return updates
         */
        @Override
        public Collection<Topic> calculate(
                final Collection<Topic> existingTopics, final Collection<Topic> requiredTopics) {
            return calculators
                    .map(calculator -> calculator.calculate(existingTopics, requiredTopics))
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
        }
    }

    /** Returns those topics to update and ignores existing topics */
    public static final class UpdateCalculator implements ChangeSetCalculator {

        /**
         * Calculate changes of topics to 'create' and 'update' ops - Update ONLY supports -
         * increasing partition size (topic config) - changing topic retention (config change)
         *
         * @param existing - existing
         * @param required - set of topics that should exist
         * @return list of new topics with 'CREATE' flag
         */
        public Collection<Topic> calculate(
                final Collection<Topic> existing, final Collection<Topic> required) {

            final var existTopicsList = new ArrayList<>(existing);

            return required.stream()
                    .filter(
                            rtopic -> {
                                boolean plannedUpdates = false;
                                if (existing.contains(rtopic)) {
                                    final var existingTopic =
                                            existTopicsList.get(existTopicsList.indexOf(rtopic));
                                    if (isPartitionChange(rtopic, existingTopic)) {
                                        rtopic.messages(
                                                rtopic.messages()
                                                        + "\nUpdate partitions:"
                                                        + existingTopic.partitions()
                                                        + " -> "
                                                        + rtopic.partitions());
                                        plannedUpdates = true;
                                    }
                                    final var configChanges = configChanges(rtopic, existingTopic);
                                    configChanges.forEach(
                                            change ->
                                                    rtopic.messages(
                                                            rtopic.messages()
                                                                    + "\nUpdate config "
                                                                    + change.getKey()
                                                                    + ":"
                                                                    + existingTopic
                                                                            .config()
                                                                            .get(change.getKey())
                                                                    + " -> "
                                                                    + change.getValue()));
                                    plannedUpdates = plannedUpdates || !configChanges.isEmpty();
                                }
                                return plannedUpdates;
                            })
                    .map(dTopic -> dTopic.state(Status.STATE.UPDATE))
                    .collect(Collectors.toList());
        }

        private static boolean isPartitionChange(final Topic requested, final Topic existing) {
            return requested.partitions() > existing.partitions();
        }

        private static List<Map.Entry<String, String>> configChanges(
                final Topic requested, final Topic existing) {
            return requested.config().entrySet().stream()
                    .filter(
                            requestedChange ->
                                    existing.config().containsKey(requestedChange.getKey())
                                            && !existing.config()
                                                    .get(requestedChange.getKey())
                                                    .equals(requestedChange.getValue()))
                    .collect(Collectors.toList());
        }
    }

    /** Returns those topics to create and ignores existing topics */
    public static final class CreateCalculator implements ChangeSetCalculator {

        /**
         * Calculate changes of topics to 'create' (ignored updates)
         *
         * @param existingTopics - existing
         * @param requiredTopics - set of topics that should exist
         * @return list of new topics with 'CREATE' flag
         */
        public Collection<Topic> calculate(
                final Collection<Topic> existingTopics, final Collection<Topic> requiredTopics) {
            return requiredTopics.stream()
                    .filter(dTopic -> !existingTopics.contains(dTopic))
                    .map(dTopic -> dTopic.state(Status.STATE.CREATE))
                    .collect(Collectors.toList());
        }
    }

    /** Returns those topics to create and ignores existing topics */
    public static final class UnspecifiedCalculator implements ChangeSetCalculator {

        /**
         * Calculate changes of topics that are unspecified
         *
         * @param existingTopics - existing
         * @param requiredTopics - set of topics that should exist
         * @return list of new topics with 'CREATE' flag
         */
        public Collection<Topic> calculate(
                final Collection<Topic> existingTopics, final Collection<Topic> requiredTopics) {
            existingTopics.removeAll(requiredTopics);
            return existingTopics;
        }
    }

    /** Main API */
    interface ChangeSetCalculator {
        /**
         * Returns changeset of topics to create/update with the 'state' flag determining which
         * actions to carry out
         *
         * @param existingTopics - existing set of topics
         * @param requiredTopics - ownership topics
         * @return - set of those that dont exist
         */
        Collection<Topic> calculate(
                Collection<Topic> existingTopics, Collection<Topic> requiredTopics);
    }

    /** Builder of the things */
    public static final class ChangeSetBuilder {

        /** defensive */
        private ChangeSetBuilder() {}

        /**
         * protected method
         *
         * @return builder
         */
        public static ChangeSetBuilder builder() {
            return new ChangeSetBuilder();
        }

        /**
         * build it
         *
         * @param cleanUnspecified - to remove unspecified resources
         * @return required calculator
         */
        public ChangeSetCalculator build(final boolean cleanUnspecified) {
            if (cleanUnspecified) {
                return new UnspecifiedCalculator();
            } else {
                return new CollectiveCalculator(new CreateCalculator(), new UpdateCalculator());
            }
        }
    }
}
