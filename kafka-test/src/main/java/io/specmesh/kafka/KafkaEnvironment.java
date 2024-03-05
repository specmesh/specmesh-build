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

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.extension.Extension;

/**
 * A Kafka environment.
 *
 * <p>Consisting of a single Kafka cluster, and the associated Schema Registry.
 */
public interface KafkaEnvironment extends Extension {

    /**
     * @return Connection string for connecting to the Kafka cluster.
     */
    String kafkaBootstrapServers();

    /**
     * @return Connection string for connecting to Schema Registry.
     */
    String schemeRegistryServer();

    /**
     * @return Returns an admin client for the Kafka cluster. Caller is responsible for closing.
     */
    Admin adminClient();

    /**
     * schema registry client
     *
     * @return srClient
     */
    SchemaRegistryClient srClient();
}
