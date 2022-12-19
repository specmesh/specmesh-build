package io.specmesh.apiparser.model;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.Value;
import lombok.experimental.Accessors;

/**
 * <a href=
 * "https://www.asyncapi.com/docs/reference/specification/v2.4.0#messageObject">...</a>
 */
@Value
@Accessors(fluent = true)
@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
@SuppressFBWarnings
@SuppressWarnings({"unchecked", "rawtypes"})
public class Message {
    @JsonProperty
    String messageId;

    @JsonProperty
    Map headers = Collections.EMPTY_MAP;

    @JsonProperty
    Map payload = Collections.EMPTY_MAP;

    @JsonProperty
    Map correlationId = Collections.EMPTY_MAP;

    @JsonProperty
    String schemaFormat;

    @JsonProperty
    String contentType;

    @JsonProperty
    String name;

    @JsonProperty
    String title;

    @JsonProperty
    String summary;

    @JsonProperty
    String description;

    @JsonProperty
    List<Tag> tags = Collections.EMPTY_LIST;

    @JsonProperty
    Bindings bindings;

    @JsonProperty
    Map traits = Collections.EMPTY_MAP;

    public String schemaRef() {
        return (String) payload.get("$ref");
    }

}
