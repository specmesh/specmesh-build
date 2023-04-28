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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.specmesh.kafka.KafkaApiSpec;
import io.specmesh.kafka.provision.Provisioner;
import io.specmesh.kafka.provision.Status;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Option;

/** SpecMesh Kafka Provisioner */
@Command(
        name = "provision",
        description =
                "Apply the provided specification to provision kafka resources and permissions on"
                        + " the cluster")
public class Provision implements Callable<Integer> {

    private Status state;

    /**
     * Main method
     *
     * @param args args
     */
    public static void main(final String[] args) {
        System.exit(new CommandLine(new Provision()).execute(args));
    }

    @Option(
            names = {"-bs", "--bootstrap-server"},
            description = "Kafka bootstrap server url")
    private String brokerUrl = "";

    @Option(
            names = {"-sr", "--srUrl"},
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
            names = {"-s", "--secret"},
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
    public Integer call() throws Exception {
        final var status =
                Provisioner.provision(
                        dryRun,
                        specMeshSpec(),
                        schemaPath,
                        Utils.adminClient(brokerUrl, username, secret),
                        Utils.schemaRegistryClient(schemaRegistryUrl, srApiKey, srApiSecret));

        final var mapper =
                new ObjectMapper()
                        .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
                        .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        System.out.println(mapper.writeValueAsString(status));
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
        return Utils.loadFromClassPath(spec, Provision.class.getClassLoader());
    }
}
