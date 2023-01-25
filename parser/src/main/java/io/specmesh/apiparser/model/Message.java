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
 * Pojo representing a Message.
 *
 * @see <a href=
 *      "https://www.asyncapi.com/docs/reference/specification/v2.4.0#messageObject">spec
 *      docs</a>
 */
@Value
@Accessors(fluent = true)
@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
@SuppressFBWarnings
@SuppressWarnings({"unchecked", "rawtypes"})
public class Message {
    @JsonProperty
    private String messageId;

    @JsonProperty
    private Map headers = Collections.EMPTY_MAP;

    @JsonProperty
    private Map payload = Collections.EMPTY_MAP;

    @JsonProperty
    private Map correlationId = Collections.EMPTY_MAP;

    @JsonProperty
    private String schemaFormat;

    @JsonProperty
    private String contentType;

    @JsonProperty
    private String name;

    @JsonProperty
    private String title;

    @JsonProperty
    private String summary;

    @JsonProperty
    private String description;

    @JsonProperty
    private List<Tag> tags = Collections.EMPTY_LIST;

    @JsonProperty
    private Bindings bindings;

    @JsonProperty
    private Map traits = Collections.EMPTY_MAP;

    /**
     * @return the location of the schema
     */
    public String schemaRef() {
        return (String) payload.get("$ref");
    }
}
