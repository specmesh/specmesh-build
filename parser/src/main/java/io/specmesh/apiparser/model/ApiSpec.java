package io.specmesh.apiparser.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Value;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Value
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ApiSpec {
    private static final char DELIMITER = System.getProperty("DELIMITER", ".").charAt(0);
    @JsonProperty
    String id;
    @JsonProperty
    String asyncapi;

    @JsonProperty
    Map<String, Map<String, Operation>> channels;

    public String id() {
        return validate(id).substring("urn:".length()).replace(":", DELIMITER+"");
    }


    private String validate(final String id) {
        if (!id.startsWith("urn:")) {
            throw new IllegalStateException("ID must be formatted as a URN, expecting urn:");
        }
        return id;
    }

    public Map<String, Channel> channels() {
        return channels.entrySet().stream().collect(Collectors.toMap(
                        e -> getCanonical(id(), e.getKey()),
                        e-> new Channel(e.getKey(), getCanonical(id(), e.getKey()),
                                Channel.PUBSUB.valueOf(e.getValue().entrySet().iterator().next().getKey().toUpperCase()),
                                e.getValue().entrySet().iterator().next().getValue()),
                        (k, v) -> k,
                        LinkedHashMap::new
                )
        );
    }

    public String asyncapi() {
        return asyncapi;
    }

    private String getCanonical(final String id, final String channelName) {
        if (channelName.startsWith("/")) {
            return channelName.substring(1).replace('/', DELIMITER);
        } else {
            return id + DELIMITER + channelName.replace('/', DELIMITER);
        }
    }
}
