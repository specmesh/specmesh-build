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
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.specmesh.apiparser.AsyncApiParser;
import io.specmesh.kafka.KafkaApiSpec;
import io.specmesh.kafka.provision.Provisioner;
import io.specmesh.kafka.provision.Status;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import picocli.CommandLine.Option;

/** SpecMesh Kafka Provisioner */
@Command(
        name = "provision",
        description =
                "Apply the provided specification to provision kafka resources and permissions on"
                        + " the cluster")
public class KafkaProvisionCommand implements Callable<Status> {

    @Option(
            names = {"-bs", "--bootstrap-server"},
            description = "Kafka bootstrap server url")
    private String brokerUrl = "";

    @Option(
            names = {"-sr", "--schemaRegistryUrl"},
            description = "schemaRegistryUrl")
    private String schemaRegistryUrl;

    @Option(
            names = {"-srKey", "--srApiKey"},
            description = "srApiKey for schema registry")
    private String srApiKey;

    @Option(
            names = {"-srSecret", "--srApiSecret"},
            description = "srApiSecret for schema secret")
    private String srApiSecret;

    @Option(
            names = {"-schemaPath", "--schemaPath"},
            description = "schemaPath where the set of referenced schemas will be loaded")
    private String schemaPath;

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

    @Option(
            names = {"-d", "--dry-run"},
            description =
                    "Compares the cluster against the spec, outputting proposed changes if"
                        + " compatible.If the spec incompatible with the cluster (not sure how it"
                        + " could be) then will fail with a descriptive error message.A return"
                        + " value of 0=indicates no changes needed; 1=changes needed; -1=not"
                        + " compatible, blah blah")
    private boolean dryRun;

    @Override
    public Status call() throws Exception {
        return Provisioner.provision(
                dryRun, specMeshSpec(), schemaPath, adminClient(), schemaRegistryClient());
    }

    private KafkaApiSpec specMeshSpec() {
        return loadFromClassPath(spec, KafkaProvisionCommand.class.getClassLoader());
    }

    private Optional<SchemaRegistryClient> schemaRegistryClient() {
        if (schemaRegistryUrl != null) {
            final Map<String, Object> properties = new HashMap<>();
            properties.put(SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
            properties.put(
                    SchemaRegistryClientConfig.USER_INFO_CONFIG, srApiKey + ":" + srApiSecret);
            return Optional.of(new CachedSchemaRegistryClient(schemaRegistryUrl, 5, properties));
        } else {
            return Optional.empty();
        }
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
