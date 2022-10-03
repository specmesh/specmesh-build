package io.specmesh.apiparser.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Value;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * https://www.asyncapi.com/docs/reference/specification/v2.4.0#operationObject
 * https://www.asyncapi.com/docs/reference/specification/v2.4.0#messageTraitObject
 */
@Value
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Operation {
    private String operationId;
    private String summary;
    private String description;
    private List<String> tags;

    @SuppressWarnings("rawtypes")
    private Map bindings;
    //    https://www.asyncapi.com/docs/reference/specification/v2.4.0#operationTraitObject
    @SuppressWarnings("rawtypes")
    private Map traits;

    private Message message;

    public String operationId() {
        return operationId;
    }

    public String summary() {
        return summary;
    }

    public String description() {
        return description;
    }

    public List<String> tags() {
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

    public Message message() {
        return message;
    }

    @Override
    public String toString() {
        return "Operation{" +
                "operationId='" + operationId + '\'' +
                ", summary='" + summary + '\'' +
                ", description='" + description + '\'' +
                ", tags=" + tags +
                ", bindings=" + bindings +
                ", traits=" + traits +
                ", message=" + message +
                '}';
    }

    @Override
    @SuppressWarnings("LineLength")
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Operation)) {
            return false;
        }
        final Operation operation = (Operation) o;
        return Objects.equals(operationId(), operation.operationId()) && Objects.equals(summary(), operation.summary()) && Objects.equals(description(), operation.description()) && Objects.equals(tags(), operation.tags()) && Objects.equals(bindings(), operation.bindings()) && Objects.equals(traits(), operation.traits()) && Objects.equals(message(), operation.message());
    }

    @Override
    public int hashCode() {
        return Objects.hash(operationId(), summary(), description(), tags(), bindings(), traits(), message());
    }
}
