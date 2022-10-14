package io.specmesh.kafka;

import io.specmesh.apiparser.model.ApiSpec;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Kafka entity mappings from the AsyncAPISpec
 *
 * TODO: consider a compareXXX function. i.e. compareTopics, compareACLs, compareSchemas
 * This could be used to drive AlterConfigsResult alterConfigs(Map<ConfigResource, Config> configs)
 * Validation checks.
 * 1 - Owned topics require a Kafka binding
 */
public class KafkaApiSpec {
    private ApiSpec apiSpec;

    public KafkaApiSpec(final ApiSpec apiSpec) {
        this.apiSpec = apiSpec;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    /**
     * Used by AdminClient.createTopics (includes support for configs overrides i.e. min.insync.replicas
     */
    public List<NewTopic> listDomainOwnedTopics() {

        validateTopicConfig();

        final String id = apiSpec.id();

        return apiSpec.channels().entrySet().stream()
                .filter(e -> e.getKey().startsWith(id))
                .map(e -> new NewTopic(
                        e.getKey(),
                        e.getValue().bindings().kafka().partitions(),
                        (short) e.getValue().bindings().kafka().replicas()
                )
                .configs(e.getValue().bindings().kafka().configs()))
                .collect(Collectors.toList());
    }

    private void validateTopicConfig() {
        final String id = apiSpec.id();
        apiSpec.channels().forEach( (k , v) -> {
                            if (k.startsWith(id) && (v.bindings() == null || v.bindings().kafka() == null)) {
                                throw new IllegalStateException("Kafka bindings are missing from channel: [" + k + "] Domain owner: [" + id + "]");
                            }
                        });
    }
}
