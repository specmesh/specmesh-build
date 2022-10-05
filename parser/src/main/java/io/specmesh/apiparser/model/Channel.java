package io.specmesh.apiparser.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Value;

@Value
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Channel {

    @JsonProperty
    String description;

    @JsonProperty
    Bindings bindings;
    @JsonProperty
    Operation publish;
    @JsonProperty
    Operation subscribe;

    public String description() {
        return description;
    }

    public Bindings bindings() {
        return bindings;
    }

    public Operation getPublish() {
        return publish;
    }

    public Operation getSubscribe() {
        return subscribe;
    }
}
