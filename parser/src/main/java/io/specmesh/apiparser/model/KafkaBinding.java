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

package io.specmesh.apiparser.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/*
 * Note: Lombok has a Value/Defaults bug which means we have messy accessors to
 * cope with default values: See - <a href=
 * "https://stackoverflow.com/questions/47883931/default-value-in-lombok-how-to-init-default-with-both-constructor-and-builder">...</a>
 * - <a href="https://github.com/projectlombok/lombok/issues/1347">...</a>
 */

/** Pojo representing a Kafka binding */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@Builder
@Data
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Accessors(fluent = true)
@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressFBWarnings
public class KafkaBinding {

    @Builder.Default @JsonProperty private List<String> envs = List.of();

    @Builder.Default @JsonProperty private Optional<Integer> partitions = Optional.empty();

    @Builder.Default @JsonProperty private Optional<Short> replicas = Optional.empty();

    @Builder.Default @JsonProperty private Map<String, String> configs = Map.of();

    @Builder.Default @JsonProperty private String groupId = "";

    @Builder.Default @JsonProperty private String schemaIdLocation = "payload";

    @Builder.Default @JsonProperty private String schemaIdPayloadEncoding = "";

    @Builder.Default @JsonProperty private String schemaLookupStrategy = "TopicNameStrategy";

    @Builder.Default @JsonProperty private String bindingVersion = "latest";

    @Builder.Default @JsonProperty private Optional<RecordPart> key = Optional.empty();

    /**
     * @return configs
     */
    public Map<String, String> configs() {
        return Map.copyOf(configs);
    }

    /**
     * @return envs
     */
    public List<String> envs() {
        return List.copyOf(envs);
    }
}
