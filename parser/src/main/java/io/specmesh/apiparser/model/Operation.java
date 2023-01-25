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
 *      "https://www.asyncapi.com/docs/reference/specification/v2.4.0#operationObject">operationObject</a>
 * @see <a href=
 *      "https://www.asyncapi.com/docs/reference/specification/v2.4.0#messageTraitObject">messageTraitObject</a>
 */
@Value
@Accessors(fluent = true)
@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
@SuppressFBWarnings
@SuppressWarnings({"unchecked", "rawtypes"})
public class Operation {
    @JsonProperty
    private String operationId;

    @JsonProperty
    private String summary;

    @JsonProperty
    private String description;

    @SuppressWarnings({"rawtypes", "unchecked"})
    @JsonProperty
    private List<Tag> tags = Collections.EMPTY_LIST;

    @JsonProperty
    private Bindings bindings;

    // https://www.asyncapi.com/docs/reference/specification/v2.4.0#operationTraitObject
    @JsonProperty
    private Map traits = Collections.EMPTY_MAP;

    @JsonProperty
    private Message message;

    /**
     * @return schema info
     */
    public SchemaInfo schemaInfo() {
        if (message.bindings() == null) {
            throw new IllegalStateException("Bindings not found for operation: " + operationId);
        }
        return new SchemaInfo(message().schemaRef(), message().schemaFormat(),
                message.bindings().kafka().schemaIdLocation(), message().contentType(),
                message.bindings().kafka().schemaLookupStrategy());
    }
}
