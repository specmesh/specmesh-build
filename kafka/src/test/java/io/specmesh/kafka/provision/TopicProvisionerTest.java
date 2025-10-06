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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.specmesh.kafka.KafkaApiSpec;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TopicProvisionerTest {

    @Mock private Admin adminClient;
    @Mock private KafkaApiSpec apiSpec;
    @Mock private NewTopic topic;

    @Test
    void shouldNotSwallowExceptions() {
        final var topic = TopicProvisioner.Topic.builder().build();
        final var swallowed =
                Status.builder()
                        .topics(
                                List.of(
                                        topic.exception(
                                                new RuntimeException(
                                                        new RuntimeException("swallowed")))))
                        .build();
        assertThat(
                swallowed.topics().iterator().next().exception().toString(),
                containsString(".java:"));
    }

    @ParameterizedTest
    @ValueSource(doubles = {-1.0, -0.1, 0.0, 1.00001})
    void shouldThrowOnInvalidPartitionCountFactor(final double partitionCountFactor) {
        // Given:
        setUpMocks(false);

        // When:
        final Exception e =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                TopicProvisioner.provision(
                                        false, false, partitionCountFactor, apiSpec, adminClient));

        // Then:
        assertThat(
                e.getMessage(),
                is("0.0 < partitionCountFactor <= 1.0, but was: " + partitionCountFactor));
    }

    @ParameterizedTest
    @ValueSource(doubles = {0.0000001, 0.1, 0.5, 1.0})
    void shouldSupportValidPartitionCountFactors(final double partitionCountFactor) {
        // Given:
        setUpMocks(true);
        when(topic.numPartitions()).thenReturn(10);

        // When:
        TopicProvisioner.provision(false, false, partitionCountFactor, apiSpec, adminClient);

        // Then:
        verify(adminClient).createTopics(any());
    }

    @Test
    void shouldNotFactoryTopicsWithOnePartition() {
        // Given:
        setUpMocks(true);
        when(topic.numPartitions()).thenReturn(1);

        // When:
        TopicProvisioner.provision(false, false, 0.0001, apiSpec, adminClient);

        // Then:
        verify(adminClient)
                .createTopics(List.of(new NewTopic(topic.name(), 1, (short) 3).configs(Map.of())));
    }

    @Test
    void shouldNeverFactoryTopicsWithMoreOnePartitionBelowTwoPartitions() {
        // Given:
        setUpMocks(true);
        when(topic.numPartitions()).thenReturn(10);

        // When:
        TopicProvisioner.provision(false, false, 0.0001, apiSpec, adminClient);

        // Then:
        verify(adminClient)
                .createTopics(List.of(new NewTopic(topic.name(), 2, (short) 3).configs(Map.of())));
    }

    private void setUpMocks(final boolean forSuccess) {
        when(apiSpec.listDomainOwnedTopics()).thenReturn(List.of(topic));
        when(topic.name()).thenReturn("bob");

        if (forSuccess) {
            when(topic.replicationFactor()).thenReturn((short) 3);

            final ListTopicsResult listTopicResult = mock();
            final KafkaFutureImpl<Collection<TopicListing>> listTopicsFuture =
                    new KafkaFutureImpl<>();
            listTopicsFuture.complete(List.of());
            when(listTopicResult.listings()).thenReturn(listTopicsFuture);
            when(adminClient.listTopics()).thenReturn(listTopicResult);

            final CreateTopicsResult createTopicResult = mock();
            final KafkaFutureImpl<Void> createTopicsFuture = new KafkaFutureImpl<>();
            createTopicsFuture.complete(null);
            when(createTopicResult.all()).thenReturn(createTopicsFuture);
            when(adminClient.createTopics(any())).thenReturn(createTopicResult);
        }
    }
}
