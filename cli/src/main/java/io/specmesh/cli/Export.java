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
import com.fasterxml.jackson.databind.SerializationFeature;
import io.specmesh.apiparser.model.ApiSpec;
import io.specmesh.apiparser.parse.SpecMapper;
import io.specmesh.kafka.Clients;
import io.specmesh.kafka.Exporter;
import java.util.Map;
import java.util.concurrent.Callable;
import lombok.Builder;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.admin.Admin;
import picocli.CommandLine;
import picocli.CommandLine.Option;

/** Basic/incomplete export of the spec-api using a app-id prefix */
@Command(name = "export", description = "Build an incomplete spec from a running Cluster")
@Getter
@Accessors(fluent = true)
@Builder
public class Export implements Callable<Integer> {

    private ApiSpec state;

    /**
     * Main method
     *
     * @param args args
     */
    public static void main(final String[] args) {
        System.exit(new CommandLine(Export.builder().build()).execute(args));
    }

    @Option(
            names = {"-bs", "--bootstrap-server"},
            description = "Kafka bootstrap server url")
    @Builder.Default
    private String brokerUrl = "";

    @Option(
            names = {"-id", "--domain-id"},
            description = "specmesh - domain/prefix - domain identified (app-id) to export against")
    private String aggid;

    @Option(
            names = {"-u", "--username"},
            description = "username or api key for the cluster connection")
    private String username;

    @Option(
            names = {"-s", "--secret"},
            description = "secret credential for the cluster connection")
    private String secret;

    @Option(
            names = "-D",
            mapFallbackValue = "",
            description =
                    "Specify Java runtime system properties for Apache Kafka. Note: bulk properties"
                            + " can be set via '-Dconfig.properties=somefile.properties"
                            + " ") // allow -Dkey
    void setProperty(final Map<String, String> props) {
        props.forEach((k, v) -> System.setProperty(k, v));
    }

    @Override
    public Integer call() throws Exception {

        try (Admin adminClient = Clients.adminClient(brokerUrl, username, secret)) {
            final var apiSpec = Exporter.export(aggid, adminClient);
            final var mapper =
                    SpecMapper.mapper()
                            .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
                            .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
            System.out.println(mapper.writeValueAsString(apiSpec));
            this.state = apiSpec;
            return 0;
        }
    }

    /**
     * get processed spec
     *
     * @return processed spec
     */
    public ApiSpec state() {
        return this.state;
    }
}
