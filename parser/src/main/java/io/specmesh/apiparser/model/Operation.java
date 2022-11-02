package io.specmesh.apiparser.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.Value;
import lombok.experimental.Accessors;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * https://www.asyncapi.com/docs/reference/specification/v2.4.0#operationObject
 * https://www.asyncapi.com/docs/reference/specification/v2.4.0#messageTraitObject
 */
@Value
@Accessors(fluent=true)
@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
@SuppressFBWarnings
@SuppressWarnings({"unchecked", "rawtypes"})
public class Operation {
    @JsonProperty
    String operationId;

    @JsonProperty
    String summary;

    @JsonProperty
    String description;

    @SuppressWarnings({"rawtypes", "unchecked"})
    @JsonProperty
    List<Tag> tags = Collections.EMPTY_LIST;

    @JsonProperty
    Bindings bindings;

    //    https://www.asyncapi.com/docs/reference/specification/v2.4.0#operationTraitObject
    @JsonProperty
    Map traits = Collections.EMPTY_MAP;

    @JsonProperty
    Message message;

    public SchemaInfo schemaInfo() {
        if (message.bindings() == null) {
            throw new IllegalStateException("Bindings not found for operation: " + operationId);
        }
        return new SchemaInfo(message().schemaRef(), message().schemaFormat(), message.bindings().kafka().schemaIdLocation(),
                message.bindings().kafka().schemaIdPayloadEncoding(), message().contentType(),
                message.bindings().kafka().schemaLookupStrategy());
    }
}
