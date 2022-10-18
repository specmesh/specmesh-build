package io.specmesh.apiparser.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.Value;
import lombok.experimental.Accessors;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Value
@Accessors(fluent=true)
@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
@SuppressFBWarnings
public class ApiSpec {
    private static final char DELIMITER = System.getProperty("DELIMITER", ".").charAt(0);
    @JsonProperty
    String id;

    @JsonProperty
    String version;

    @JsonProperty
    String asyncapi;

    @JsonProperty
    Map<String, Channel> channels;

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
                        e-> e.getValue(),
                        (k, v) -> k,
                        LinkedHashMap::new
                )
        );
    }

    private String getCanonical(final String id, final String channelName) {
        if (channelName.startsWith("/")) {
            return channelName.substring(1).replace('/', DELIMITER);
        } else {
            return id + DELIMITER + channelName.replace('/', DELIMITER);
        }
    }
}
