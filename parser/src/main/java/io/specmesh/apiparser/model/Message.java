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
    private Map headers;
    private Map payload;
    private Map correlationId;
    private String schemaFormat;
    private String contentType;
    private String name;
    private String title;
    private String summary;
    private String description;
    private Map tags;
    private Map bindings;
    private Map traits;

    public String messageId() {
        return messageId;
    }

    public Map headers() {
        return headers;
    }

    public Map payload() {
        return payload;
    }

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

    public Map tags() {
        return tags;
    }

    public Map bindings() {
        return bindings;
    }

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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Message)) return false;
        Message message = (Message) o;
        return Objects.equals(messageId(), message.messageId()) && Objects.equals(headers(), message.headers()) && Objects.equals(payload(), message.payload()) && Objects.equals(correlationId(), message.correlationId()) && Objects.equals(schemaFormat(), message.schemaFormat()) && Objects.equals(contentType(), message.contentType()) && Objects.equals(name(), message.name()) && Objects.equals(title(), message.title()) && Objects.equals(summary(), message.summary()) && Objects.equals(description(), message.description()) && Objects.equals(tags(), message.tags()) && Objects.equals(bindings(), message.bindings()) && Objects.equals(traits(), message.traits());
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageId(), headers(), payload(), correlationId(), schemaFormat(), contentType(), name(), title(), summary(), description(), tags(), bindings(), traits());
    }
}
