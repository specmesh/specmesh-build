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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.specmesh.kafka.provision.TopicProvisioner.Topic;
import io.specmesh.kafka.provision.TopicWriters.UpdateWriter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TopicWritersTest {

    @Mock Admin client;

    @Test
    void shouldWriteUpdatesForRetentionChange() throws Exception {
        final var topicWriter = new UpdateWriter(client);

        // Mock hell - crappy admin api
        final var topicDescriptionFuture = mock(KafkaFuture.class);
        when(topicDescriptionFuture.get(Provisioner.REQUEST_TIMEOUT, TimeUnit.SECONDS))
                .thenReturn(getTopicDescription());

        final var describeResult = mock(DescribeTopicsResult.class);

        // test value of existing topic
        when(describeResult.topicNameValues()).thenReturn(Map.of("test", topicDescriptionFuture));

        when(client.describeTopics(List.of("test"))).thenReturn(describeResult);

        // test
        final var updates =
                List.of(
                        Topic.builder()
                                .name("test")
                                .state(Status.STATE.UPDATE)
                                .partitions(1)
                                .config(Map.of(TopicConfig.RETENTION_MS_CONFIG, "1000"))
                                .build());
        final var updated = topicWriter.write(updates);
        final var next = updated.iterator().next();

        // verify
        assertThat(next.state(), is(Status.STATE.UPDATED));
        assertThat(next.messages(), is(containsString(TopicConfig.RETENTION_MS_CONFIG)));
        assertThat(next.config().get(TopicConfig.RETENTION_MS_CONFIG), is("1000"));
    }

    @Test
    void shouldWriteUpdatesToPartitionsWhenLarger() throws Exception {
        final var topicWriter = new UpdateWriter(client);

        // Mock hell - crappy admin api
        final var topicDescriptionFuture = mock(KafkaFuture.class);
        when(topicDescriptionFuture.get(Provisioner.REQUEST_TIMEOUT, TimeUnit.SECONDS))
                .thenReturn(getTopicDescription());

        final var describeResult = mock(DescribeTopicsResult.class);

        // test value of existing topic
        when(describeResult.topicNameValues()).thenReturn(Map.of("test", topicDescriptionFuture));

        when(client.describeTopics(List.of("test"))).thenReturn(describeResult);

        final var createPartitionsResult = mock(CreatePartitionsResult.class);
        final var createPartitionFuture = mock(KafkaFuture.class);
        when(createPartitionsResult.all()).thenReturn(createPartitionFuture);
        when(createPartitionFuture.get(Provisioner.REQUEST_TIMEOUT, TimeUnit.SECONDS))
                .thenReturn(Void.class);
        when(client.createPartitions(any(), any())).thenReturn(createPartitionsResult);

        // test
        final var updates =
                List.of(
                        Topic.builder()
                                .name("test")
                                .state(Status.STATE.UPDATE)
                                .partitions(999)
                                .build());
        final var updated = topicWriter.write(updates);
        final var next = updated.iterator().next();

        // verify
        assertThat(next.state(), is(Status.STATE.UPDATED));
        assertThat(next.messages(), is(containsString("Updated partitionCount")));
        assertThat(next.partitions(), is(999));
    }

    private static TopicDescription getTopicDescription() {
        return new TopicDescription(
                "test",
                false,
                List.of(
                        new TopicPartitionInfo(
                                1, new Node(1, "host", 33000), List.of(), List.of())));
    }
}
