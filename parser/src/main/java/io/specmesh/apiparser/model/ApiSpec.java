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

    public Map<String, Map<String, Operation>> channels() {
        return new LinkedHashMap<>(channels);
    }

    public String asyncapi() {
        return asyncapi;
    }

    /**
     * Returns list of channel names
     * @return fully qualified canonical channel names (id + channel-name) unless fully qualified already
     */
    public Map<String, String> canonicalChannels() {
        return channels.entrySet().stream().collect(
                Collectors.toMap(
                    e -> getCanonical(id(), e.getKey()),
                        Map.Entry::getKey,
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
