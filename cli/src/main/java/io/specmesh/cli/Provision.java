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

import com.google.common.annotations.VisibleForTesting;
import io.specmesh.kafka.provision.Provisioner;
import io.specmesh.kafka.provision.Status;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Option;

/** SpecMesh Kafka Provisioner */
@SuppressWarnings("unused")
@Command(
        name = "provision",
        description =
                "Apply a specification.yaml to provision kafka resources on a cluster.\n"
                        + "Use 'provision.properties' for common arguments\n"
                        + " Explicit properties file location /app/provision.properties\n\n")
public final class Provision implements Callable<Integer> {

    private final Provisioner.ProvisionerBuilder builder = Provisioner.builder();

    private Status status;

    @VisibleForTesting
    Provision() {}

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
                new CommandLine(new Provision()).setDefaultValueProvider(provider).execute(args));
    }

    @Option(
            names = {"-bs", "--bootstrap-server"},
            description = "Kafka bootstrap server url")
    public void brokerUrl(final String brokerUrl) {
        builder.brokerUrl(brokerUrl);
    }

    @Option(
            names = {"-srDisabled", "--sr-disabled"},
            description = "Ignore schema related operations")
    public void srDisabled(final boolean disable) {
        builder.srDisabled(disable);
    }

    @Option(
            names = {"-aclDisabled", "--acl-disabled"},
            description = "Ignore ACL related operations")
    public void aclDisabled(final boolean disable) {
        builder.aclDisabled(disable);
    }

    @Option(
            names = {"-sr", "--schema-registry"},
            description = "schemaRegistryUrl")
    public void schemaRegistryUrl(final String url) {
        builder.schemaRegistryUrl(url);
    }

    @Option(
            names = {"-srKey", "--sr-api-key"},
            description = "srApiKey for schema registry")
    public void srApiKey(final String key) {
        builder.srApiKey(key);
    }

    @Option(
            names = {"-srSecret", "--sr-api-secret"},
            description = "srApiSecret for schema secret")
    public void srApiSecret(final String secret) {
        builder.srApiSecret(secret);
    }

    @Option(
            names = {"-schemaPath", "--schema-path"},
            description = "schemaPath where the set of referenced schemas will be loaded")
    public void schemaPath(final String path) {
        builder.schemaPath(path);
    }

    @Option(
            names = {"-spec", "--spec"},
            description = "specmesh specification file")
    public void spec(final String path) {
        builder.specPath(path);
    }

    @Option(
            names = {"-du", "--domain-user"},
            description =
                    "optional custom domain user, to be used when creating ACLs. By default,"
                        + " specmesh expects the principle used to authenticate with Kafka to have"
                        + " the same name as the domain id. For example, given a domain id of"
                        + " 'urn:acme.products', specmesh expects the user to be called"
                        + " 'acme.products', and creates ACLs accordingly. In some situations, e.g."
                        + " Confluent Cloud Service Accounts, the username is system generated or"
                        + " outside control of administrators.  In these situations, use this"
                        + " option to provide the generated username and specmesh will provision"
                        + " ACLs accordingly.")
    public void domainUserAlias(final String alias) {
        builder.domainUserAlias(alias);
    }

    @Option(
            names = {"-u", "--username"},
            description = "username or api key for the Kafka cluster connection")
    public void username(final String username) {
        builder.username(username);
    }

    @Option(
            names = {"-s", "--secret"},
            description = "secret credential for the Kafka cluster connection")
    public void secret(final String secret) {
        builder.secret(secret);
    }

    @Option(
            names = {"-dry", "--dry-run"},
            fallbackValue = "false",
            description =
                    "Compares the cluster resources against the spec, outputting proposed changes"
                            + " if  compatible. If the spec incompatible with the cluster then will"
                            + " fail with a descriptive error message. A return value of '0' ="
                            + " indicates no  changes needed; '1' = changes needed; '-1' not"
                            + " compatible")
    public void dryRun(final boolean enabled) {
        builder.dryRun(enabled);
    }

    @Option(
            names = {"-clean", "--clean-unspecified"},
            fallbackValue = "false",
            description =
                    "Compares the cluster resources against the spec, outputting proposed set of"
                        + " resources that are unexpected (not specified). Use with '-dry-run' for"
                        + " non-destructive checks. This operation will not create resources, it"
                        + " will only remove unspecified resources")
    public void cleanUnspecified(final boolean enabled) {
        builder.cleanUnspecified(enabled);
    }

    @Option(
            names = {"-D", "--property"},
            mapFallbackValue = "",
            description = "Specify Java runtime properties for Apache Kafka." + " ") // allow -Dkey
    void setProperty(final Map<String, String> props) {
        props.forEach(System::setProperty);
    }

    public Integer call() {
        this.status = builder.build().provision();
        System.out.println(status.toString());
        return status.failed() ? 1 : 0;
    }

    @VisibleForTesting
    Status state() {
        return status;
    }
}
