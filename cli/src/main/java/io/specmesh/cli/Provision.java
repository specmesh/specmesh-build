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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.specmesh.kafka.Clients;
import io.specmesh.kafka.KafkaApiSpec;
import io.specmesh.kafka.provision.Provisioner;
import io.specmesh.kafka.provision.Status;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import lombok.Builder;
import lombok.Getter;
import lombok.experimental.Accessors;
import picocli.CommandLine;
import picocli.CommandLine.Option;

/** SpecMesh Kafka Provisioner */
@Command(
        name = "provision",
        description =
                "Apply a specification.yaml to provision kafka resources on a cluster.\n"
                        + "Use 'provision.properties' for common arguments\n"
                        + " Explicit properties file location /app/provision.properties\n\n")
@Getter
@Accessors(fluent = true)
@Builder
@SuppressFBWarnings
public class Provision implements Callable<Integer> {

    private Status state;

    /**
     * Main method
     *
     * @param args args
     */
    public static void main(final String[] args) {

        final var properties = new Properties();
        final var propertyFilename =
                System.getProperty("provision.properties", "provision.properties");
        try (FileInputStream fis = new FileInputStream(propertyFilename)) {
            System.out.println(
                    "Loading `"
                            + propertyFilename
                            + "` from:"
                            + new File(propertyFilename).getAbsolutePath());
            properties.load(fis);
            properties
                    .entrySet()
                    .forEach(
                            entry -> {
                                properties.put(
                                        entry.getKey().toString().replace(".", "-"),
                                        entry.getValue());
                            });
            System.out.println(
                    "Loaded `properties` from cwd:" + new File(propertyFilename).getAbsolutePath());
        } catch (IOException e) {
            System.out.println(
                    "Missing `"
                            + propertyFilename
                            + " ` FROM:"
                            + new File(propertyFilename).getAbsolutePath()
                            + "\nERROR:"
                            + e);
            e.printStackTrace();
        }

        final var provider = new CommandLine.PropertiesDefaultProvider(properties);
        System.exit(
                new CommandLine(Provision.builder())
                        .setDefaultValueProvider(provider)
                        .execute(args));
    }

    @Option(
            names = {"-bs", "--bootstrap-server"},
            description = "Kafka bootstrap server url")
    @Builder.Default
    private String brokerUrl = "";

    @Option(
            names = {"-srDisabled", "--sr-disabled"},
            description = "Ignore schema related operations")
    private boolean srDisabled;

    @Option(
            names = {"-aclDisabled", "--acl-disabled"},
            description = "Ignore ACL related operations")
    private boolean aclDisabled;

    @Option(
            names = {"-sr", "--schema-registry"},
            description = "schemaRegistryUrl")
    private String schemaRegistryUrl;

    @Option(
            names = {"-srKey", "--sr-api-key"},
            description = "srApiKey for schema registry")
    private String srApiKey;

    @Option(
            names = {"-srSecret", "--sr-api-secret"},
            description = "srApiSecret for schema secret")
    private String srApiSecret;

    @Option(
            names = {"-schemaPath", "--schema-path"},
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
            names = {"-s", "--secret"},
            description = "secret credential for the cluster connection")
    private String secret;

    @Option(
            names = {"-dry", "--dry-run"},
            fallbackValue = "false",
            description =
                    "Compares the cluster resources against the spec, outputting proposed changes"
                            + " if  compatible. If the spec incompatible with the cluster then will"
                            + " fail with a descriptive error message. A return value of '0' ="
                            + " indicates no  changes needed; '1' = changes needed; '-1' not"
                            + " compatible")
    private boolean dryRun;

    @Option(
            names = {"-clean", "--clean-unspecified"},
            fallbackValue = "false",
            description =
                    "Compares the cluster resources against the spec, outputting proposed set of"
                        + " resources that are unexpected (not specified). Use with '-dry-run' for"
                        + " non-destructive checks. This operation will not create resources, it"
                        + " will only remove unspecified resources")
    private boolean cleanUnspecified;

    @Option(
            names = {"-D", "--property"},
            mapFallbackValue = "",
            description = "Specify Java runtime properties for Apache Kafka." + " ") // allow -Dkey
    void setProperty(final Map<String, String> props) {
        props.forEach(System::setProperty);
    }

    @Override
    public Integer call() throws Exception {
        final var status =
                Provisioner.provision(
                        !aclDisabled,
                        dryRun,
                        cleanUnspecified,
                        specMeshSpec(),
                        schemaPath,
                        Clients.adminClient(brokerUrl, username, secret),
                        Clients.schemaRegistryClient(
                                !srDisabled, schemaRegistryUrl, srApiKey, srApiSecret));

        System.out.println(status.toString());
        this.state = status;
        return 0;
    }

    /**
     * get processed state
     *
     * @return processed state
     */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "omg")
    public Status state() {
        return state;
    }

    private KafkaApiSpec specMeshSpec() {
        return KafkaApiSpec.loadFromClassPath(spec, Provision.class.getClassLoader());
    }
}
