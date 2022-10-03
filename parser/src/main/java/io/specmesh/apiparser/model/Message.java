package io.specmesh.apiparser.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Value;

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
    String messageId;

    @SuppressWarnings("rawtypes")
    Map headers;

    @SuppressWarnings("rawtypes")
    Map payload;

    @SuppressWarnings("rawtypes")
    Map correlationId;
    String schemaFormat;
    String contentType;
    String name;
    String title;
    String summary;
    String description;
    @SuppressWarnings("rawtypes")
    Map tags;
    @SuppressWarnings("rawtypes")
    Map bindings;
    @SuppressWarnings("rawtypes")
    Map traits;

    public String messageId() {
        return messageId;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public Map headers() {
        return new LinkedHashMap<>(headers);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public Map payload() {
        return new LinkedHashMap<>(payload);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public Map correlationId() {
        return new LinkedHashMap<>(correlationId);
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
        return new LinkedHashMap(tags);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public Map bindings() {
        return new LinkedHashMap(bindings);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public Map traits() {
        return new LinkedHashMap(traits);
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
