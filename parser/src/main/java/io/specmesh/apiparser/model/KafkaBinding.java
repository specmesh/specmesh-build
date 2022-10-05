package io.specmesh.apiparser.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Value;

import java.util.List;


@Value
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class KafkaBinding {
    @JsonProperty
    List<String> envs;
    @JsonProperty
    int partitions;
    @JsonProperty
    int replicas;
    @JsonProperty
    int retention;

    @JsonProperty
    String groupId;

    public List<String> envs() {
        return envs;
    }

    public int partitions() {
        return partitions;
    }

    public int replicas() {
        return replicas;
    }

    public int retention() {
        return retention;
    }

    public String groupId() {
        return groupId;
    }
}
