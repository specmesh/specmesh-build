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

package io.specmesh.cli;

import static picocli.CommandLine.Command;

import io.specmesh.apiparser.AsyncApiParser;
import io.specmesh.kafka.KafkaApiSpec;
import io.specmesh.kafka.admin.SmAdminClient;
import io.specmesh.kafka.admin.SmAdminClient.ConsumerGroup;
import io.specmesh.kafka.provision.Provisioner;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import picocli.CommandLine.Option;

/** Spec consumers stats for those groups consuming topic data */
@Command(
        name = "consumption",
        description = "Given a spec, break down the consumption volume against each of its topic")
public class SpecConsumptionCommand implements Callable<Map<String, ConsumerGroup>> {

    @Option(
            names = {"-bs", "--bootstrap-server"},
            description = "Kafka bootstrap server url")
    private String brokerUrl = "";

    @Option(
            names = {"-spec", "--spec"},
            description = "specmesh specification file")
    private String spec;

    @Option(
            names = {"-u", "--username"},
            description = "username or api key for the cluster connection")
    private String username;

    @Option(
            names = {"-p", "--secret"},
            description = "secret credential for the cluster connection")
    private String secret;

    @SuppressWarnings("checkstyle:RegexpSingleline")
    @Override
    public Map<String, ConsumerGroup> call() throws Exception {

        final var client = SmAdminClient.create(adminClient());

        final var apiSpec = specMeshSpec();
        final var topics =
                apiSpec.listDomainOwnedTopics().stream()
                        .map(NewTopic::name)
                        .collect(Collectors.toList());

        final var results = new TreeMap<String, ConsumerGroup>();

        topics.forEach(
                topic -> {
                    final var groups = client.groupsForTopicPrefix(topic);
                    groups.forEach(group -> results.put(topic, group));
                });
        return results;
    }

    private KafkaApiSpec specMeshSpec() {
        return loadFromClassPath(spec, SpecConsumptionCommand.class.getClassLoader());
    }

    /**
     * AdminClient access
     *
     * @return = adminClient
     */
    private Admin adminClient() {
        final Map<String, Object> properties = new HashMap<>();
        properties.put(AdminClientConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        properties.putAll(Provisioner.clientSaslAuthProperties(username, secret));

        return AdminClient.create(properties);
    }

    /**
     * loads the spec from the classpath
     *
     * @param spec to load
     * @param classLoader to use
     * @return the loaded spec
     */
    private static KafkaApiSpec loadFromClassPath(
            final String spec, final ClassLoader classLoader) {
        try (InputStream s = classLoader.getResourceAsStream(spec)) {
            if (s == null) {
                throw new FileNotFoundException("API Spec resource not found: " + spec);
            }
            return new KafkaApiSpec(new AsyncApiParser().loadResource(s));
        } catch (IOException e) {
            throw new RuntimeException("Failed to load API spec: " + spec, e);
        }
    }
}
