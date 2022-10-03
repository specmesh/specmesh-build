package io.specmesh.apiparser.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Value;

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
    private String messageId;

    @SuppressWarnings("rawtypes")
    private Map headers;

    @SuppressWarnings("rawtypes")
    private Map payload;

    @SuppressWarnings("rawtypes")
    private Map correlationId;
    private String schemaFormat;
    private String contentType;
    private String name;
    private String title;
    private String summary;
    private String description;
    @SuppressWarnings("rawtypes")
    private Map tags;
    @SuppressWarnings("rawtypes")
    private Map bindings;
    @SuppressWarnings("rawtypes")
    private Map traits;

    public String messageId() {
        return messageId;
    }

    @SuppressWarnings("rawtypes")
    public Map headers() {
        return headers;
    }

    @SuppressWarnings("rawtypes")
    public Map payload() {
        return payload;
    }

    @SuppressWarnings("rawtypes")
    public Map correlationId() {
        return correlationId;
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

    @SuppressWarnings("rawtypes")
    public Map tags() {
        return tags;
    }

    @SuppressWarnings("rawtypes")
    public Map bindings() {
        return bindings;
    }

    @SuppressWarnings("rawtypes")
    public Map traits() {
        return traits;
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
