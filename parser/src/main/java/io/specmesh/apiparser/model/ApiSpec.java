package io.specmesh.apiparser.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Value;

import java.util.LinkedHashMap;
import java.util.Map;

@Value
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ApiSpec {
    String id;
    String asyncapi;

    Map<String, Map<String, Operation>> channels;

    public String id() {
        return id;
    }

    public Map<String, Map<String, Operation>> channels() {
        return new LinkedHashMap<>(channels);
    }

    public String asyncapi() {
        return asyncapi;
    }

}
