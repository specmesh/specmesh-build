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
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.specmesh.kafka.KafkaApiSpec;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;
import lombok.Builder;
import lombok.Getter;
import lombok.experimental.Accessors;
import picocli.CommandLine;
import picocli.CommandLine.Option;

/** Rewrite the spec with the id prefixed to the channel.id */
@Command(
        name = "flatten",
        description =
                "Flattens the 'id' into the channel name to support fully qualified channel names")
@Getter
@Accessors(fluent = true)
@Builder
public class Flatten implements Callable<Integer> {

    /**
     * Main method
     *
     * @param args args
     */
    public static void main(final String[] args) {
        System.exit(new CommandLine(Flatten.builder().build()).execute(args));
    }

    @Option(
            names = {"-in", "--in-spec"},
            description = "Source spec to flatten")
    @Builder.Default
    private String inSpec = "";

    @Option(
            names = {"-out", "--out-spec"},
            description = "output spec")
    private String outSpec;

    @Override
    public Integer call() throws Exception {

        final var apiSpec1 = KafkaApiSpec.loadFromClassPath(inSpec, Flatten.class.getClassLoader());

        final var apiSpec = apiSpec1.apiSpec();

        final var mapper =
                new ObjectMapper(new YAMLFactory())
                        .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
                        .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        final var channels = apiSpec.channels();
        apiSpec.channels(channels);
        System.out.println(mapper.writeValueAsString(apiSpec));
        try (FileOutputStream fileOutputStream = new FileOutputStream(outSpec)) {
            fileOutputStream.write(
                    mapper.writeValueAsString(apiSpec).getBytes(StandardCharsets.UTF_8));
        }
        return 0;
    }
}
