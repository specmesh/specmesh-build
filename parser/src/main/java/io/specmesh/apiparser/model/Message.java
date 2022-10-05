package io.specmesh.apiparser.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Value;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * https://www.asyncapi.com/docs/reference/specification/v2.4.0#messageObject
 */
@Value
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Message {
    @JsonProperty
    String messageId;

    @SuppressWarnings("rawtypes")
    @JsonProperty
    Map headers;

    @SuppressWarnings("rawtypes")
    @JsonProperty
    Map payload;

    @SuppressWarnings("rawtypes")
    @JsonProperty
    Map correlationId;
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
    @SuppressWarnings("rawtypes")
    @JsonProperty
    Map tags;
    @SuppressWarnings("rawtypes")
    @JsonProperty
    Map bindings;
    @SuppressWarnings("rawtypes")
    @JsonProperty
    Map traits;

    public String messageId() {
        return messageId;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public Map headers() {
        return headers == null ? Collections.EMPTY_MAP : new LinkedHashMap<>(headers);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public Map payload() {
        return payload == null ? Collections.EMPTY_MAP : new LinkedHashMap<>(payload);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public Map correlationId() {
        return correlationId == null ? Collections.EMPTY_MAP : new LinkedHashMap<>(correlationId);
    }

    public String schemaFormat() {
        return schemaFormat;
    }

    public String contentType() {
        return contentType;
    }

    public String name() {
        return name;
    }

    public String title() {
        return title;
    }

    public String summary() {
        return summary;
    }

    public String description() {
        return description;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public Map tags() {
        return tags == null ? Collections.EMPTY_MAP : new LinkedHashMap(tags);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public Map bindings() {
        return bindings == null ? Collections.EMPTY_MAP : new LinkedHashMap(bindings);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public Map traits() {
        return traits == null ? Collections.EMPTY_MAP : new LinkedHashMap(traits);
    }

    @Override
    public String toString() {
        return "Message{" +
                "messageId='" + messageId + '\'' +
                ", headers=" + headers +
                ", payload=" + payload +
                ", correlationId=" + correlationId +
                ", schemaFormat='" + schemaFormat + '\'' +
                ", contentType='" + contentType + '\'' +
                ", name='" + name + '\'' +
                ", title='" + title + '\'' +
                ", summary='" + summary + '\'' +
                ", description='" + description + '\'' +
                ", tags=" + tags +
                ", bindings=" + bindings +
                ", traits=" + traits +
                '}';
    }

    @Override
    @SuppressWarnings({"CyclomaticComplexity", "LineLength"})
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Message)) {
            return false;
        }
        final Message message = (Message) o;
        return Objects.equals(messageId(), message.messageId()) && Objects.equals(headers(), message.headers()) && Objects.equals(payload(), message.payload()) && Objects.equals(correlationId(), message.correlationId()) && Objects.equals(schemaFormat(), message.schemaFormat()) && Objects.equals(contentType(), message.contentType()) && Objects.equals(name(), message.name()) && Objects.equals(title(), message.title()) && Objects.equals(summary(), message.summary()) && Objects.equals(description(), message.description()) && Objects.equals(tags(), message.tags()) && Objects.equals(bindings(), message.bindings()) && Objects.equals(traits(), message.traits());
    }

    @Override
    @SuppressWarnings({"LineLength"})
    public int hashCode() {
        return Objects.hash(messageId(), headers(), payload(), correlationId(), schemaFormat(), contentType(), name(), title(), summary(), description(), tags(), bindings(), traits());
    }
}
