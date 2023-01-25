package io.specmesh.apiparser.model;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.NoArgsConstructor;
import lombok.Value;
import lombok.experimental.Accessors;

/*
 * Note: Lombok has a Value/Defaults bug which means we have messy accessors to
 * cope with default values: See - <a href=
 * "https://stackoverflow.com/questions/47883931/default-value-in-lombok-how-to-init-default-with-both-constructor-and-builder">...</a>
 * - <a href="https://github.com/projectlombok/lombok/issues/1347">...</a>
 */

/**
 * Pojo representing a Kafka binding
 */
@Value
@Accessors(fluent = true)
@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor(force = true) // , access = AccessLevel.PRIVATE)
@SuppressFBWarnings
public class KafkaBinding {
    private static final int DAYS_TO_MS = 24 * 60 * 60 * 1000;
    private static final String RETENTION_MS = "retention.ms";

    @SuppressWarnings("unchecked")
    @JsonProperty
    private List<String> envs = Collections.EMPTY_LIST;

    @JsonProperty
    private int partitions;

    @JsonProperty
    private int replicas;

    @JsonProperty
    private int retention;

    @JsonProperty
    private Map<String, String> configs = Collections.emptyMap();

    @JsonProperty
    private String groupId;

    @JsonProperty
    private String schemaIdLocation;

    @JsonProperty
    private String schemaLookupStrategy;

    @JsonProperty
    private String bindingVersion;

    /**
     * @return configs
     */
    public Map<String, String> configs() {
        final Map<String, String> results = new LinkedHashMap<>(configs);

        if (!results.containsKey(RETENTION_MS) && retention != 0) {
            results.put("retention.ms", String.valueOf(retention * DAYS_TO_MS));
        }
        return results;
    }

    /**
     *
     * @return number of topic partitions
     */
    public int partitions() {
        return partitions == 0 ? 1 : partitions;
    }

    /**
     *
     * @return number of topic replicas
     */
    public int replicas() {
        return replicas == 0 ? 1 : replicas;
    }

    /**
     *
     * @return message retention in ms.
     */
    public int retention() {
        return retention == 0 ? 1 : retention;
    }
}
