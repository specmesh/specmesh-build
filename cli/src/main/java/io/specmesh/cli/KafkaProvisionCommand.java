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

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.specmesh.apiparser.AsyncApiParser;
import io.specmesh.kafka.KafkaApiSpec;
import io.specmesh.kafka.Provisioner;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import picocli.CommandLine.Option;

/**
 * SpecMesh Kafka Provisioner
 */
@Command(name = "provision", description = "Applied provided specification file to the cluster")
public class KafkaProvisionCommand implements Runnable {

    @Option(
            names = {"-br", "--brokerUrl"},
            description = "Cluster url")
    private String brokerUrl;

    @Option(
            names = {"-sr", "--schemaRegistryUrl"},
            description = "schemaRegistryUrl")
    private String schemaRegistryUrl;

    @Option(
            names = {"-srApiKey", "--srApiKey"},
            description = "srApiKey")
    private String srApiKey;

    @Option(
            names = {"-srApiSecret", "--srApiSecret"},
            description = "srApiSecret")
    private String srApiSecret;

    @Option(
            names = {"-schemaPath", "--schemaPath"},
            description = "schemaPath")
    private String schemaPath;

    @Option(
            names = {"-specmeshFile", "--specmeshFile"},
            description = "specmeshFile")
    private String specmeshFile;

    @Option(
            names = {"-username", "--username"},
            description = "username")
    private String username;

    @Option(
            names = {"-secret", "--secret"},
            description = "secret")
    private String secret;

    @Override
    public void run() {
        Provisioner.provision(specMeshSpec(), schemaPath, adminClient(), schemaRegistryClient());
    }

    private KafkaApiSpec specMeshSpec() {
        return loadFromClassPath(specmeshFile, KafkaProvisionCommand.class.getClassLoader());
    }

    private CachedSchemaRegistryClient schemaRegistryClient() {
        if (schemaRegistryUrl != null) {
            final Map<String, Object> properties = new HashMap<>();
            properties.put(SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
            properties.put(
                    SchemaRegistryClientConfig.USER_INFO_CONFIG, srApiKey + ":" + srApiSecret);
            return new CachedSchemaRegistryClient(schemaRegistryUrl, 5, properties);
        } else {
            return null;
        }
    }

    /**
     * AdminClient access
     * @return = adminClient
     */
    public Admin adminClient() {
        final Map<String, Object> properties = new HashMap<>();
        properties.put(AdminClientConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        properties.putAll(Provisioner.clientSaslAuthProperties(username, secret));

        return AdminClient.create(properties);
    }

    /**
     * loads the spec from the classpath
     * @param spec to load
     * @param classLoader to use
     * @return the loaded spec
     */
    public static KafkaApiSpec loadFromClassPath(final String spec, final ClassLoader classLoader) {
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
