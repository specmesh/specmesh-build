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
 * https://www.asyncapi.com/docs/reference/specification/v2.4.0#messageObject
 */
@Value
@Accessors(fluent=true)
@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
@SuppressFBWarnings
public class Message {
    @JsonProperty
    String messageId;

    @SuppressWarnings("rawtypes")
    @JsonProperty
    Map headers = Collections.EMPTY_MAP;;

    @SuppressWarnings("rawtypes")
    @JsonProperty
    Map payload = Collections.EMPTY_MAP;;

    @SuppressWarnings("rawtypes")
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
    List<Tag> tags = Collections.EMPTY_LIST ;
    @SuppressWarnings("rawtypes")
    @JsonProperty
    Map bindings;
    @SuppressWarnings("rawtypes")
    @JsonProperty
    Map traits = Collections.EMPTY_MAP;

}
