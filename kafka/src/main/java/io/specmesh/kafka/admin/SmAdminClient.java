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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.admin.Admin;

/** SpecMesh simple admin client api */
public interface SmAdminClient {
    /**
     * Returns the set of consumer groups (and stats) for a topicPrefix)
     *
     * @param topicPrefix to query against
     * @return matched groups
     */
    List<ConsumerGroup> groupsForTopicPrefix(String topicPrefix);

    /**
     * Report the volume of data in bytes for a topic
     *
     * @param topic to query against
     * @return topic bytes
     */
    long topicVolumeUsingLogDirs(String topic);

    /**
     * Get the set of broker ids
     *
     * @return broker Ids list
     */
    List<Integer> brokerIds();

    /**
     * Get topic offset total
     *
     * @param topic to query against
     * @return total offset count (number of records)
     */
    long topicVolumeOffsets(String topic);

    static SmAdminClient create(final Admin client) {
        return new SimpleAdminClient(client);
    }

    /** Report client level exceptions */
    class ClientException extends RuntimeException {

        ClientException(final String msg, final Throwable cause) {
            super(msg, cause);
        }
    }

    @Builder
    @Data
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    @Accessors(fluent = true)
    @SuppressFBWarnings
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    class ConsumerGroup {
        @EqualsAndHashCode.Include private String id;
        private List<Member> members;
        private List<Partition> partitions;
        private long offsetTotal;

        void calculateTotalOffset() {
            partitions.forEach(p -> offsetTotal += p.offset());
        }
    }

    @Builder
    @Data
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    @Accessors(fluent = true)
    @SuppressFBWarnings
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    class Member {
        @EqualsAndHashCode.Include private String id;
        private String clientId;
        private String host;
    }

    @Builder
    @Data
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    @Accessors(fluent = true)
    @SuppressFBWarnings
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    class Partition {
        @EqualsAndHashCode.Include private int id;
        private String topic;
        private long offset;
    }
}
