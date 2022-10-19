package io.specmesh.apiparser.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.NoArgsConstructor;
import lombok.Value;
import lombok.experimental.Accessors;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Note: Lombok has a Value/Defaults bug which means we have messy accessors to cope with default values:
 * See
 * - https://stackoverflow.com/questions/47883931/default-value-in-lombok-how-to-init-default-with-both-constructor-and-builder
 * - https://github.com/projectlombok/lombok/issues/1347
 */


@Value
@Accessors(fluent=true)
@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor(force = true)//, access = AccessLevel.PRIVATE)
@SuppressFBWarnings
public class KafkaBinding {
    public static final int DAYS_TO_MS = 24 * 60 * 60 * 1000;
    public static final String RETENTION_MS = "retention.ms";

    @SuppressWarnings( "unchecked")
    @JsonProperty
    List<String> envs = Collections.EMPTY_LIST;

    @JsonProperty
    int partitions;

    @JsonProperty
    int replicas;

    @JsonProperty
    int retention;

    @SuppressWarnings("rawtypes")
    @JsonProperty
    Map configs = Collections.EMPTY_MAP;

    @JsonProperty
    String groupId;

    @SuppressWarnings({"rawtypes", "unchecked"})
    public Map configs() {
        final Map results = new LinkedHashMap(configs != null ? configs : new HashMap());

        if (!results.containsKey(RETENTION_MS) && retention != 0) {
            results.put("retention.ms", String.valueOf(retention * DAYS_TO_MS));
        }
        return results;
    }

    public int partitions() {
        return partitions == 0 ? 1 : partitions;
    }

    public int replicas() {
        return replicas == 0 ? 1 : replicas;
    }

    public int retention() {
        return retention == 0 ? 1 : retention;
    }
}
