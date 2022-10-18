package io.specmesh.kafka;

import io.specmesh.apiparser.model.ApiSpec;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Kafka entity mappings from the AsyncAPISpec
 */
public class KafkaApiSpec {
    private ApiSpec apiSpec;

    public KafkaApiSpec(final ApiSpec apiSpec) {
        this.apiSpec = apiSpec;
    }

    public String id() {
        return apiSpec.id();
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

    /**
     * Create an ACL for the domain-id principle that allows writing to any topic prefixed with the Id
     * Prevent non ACL'd ones from writing to it (somehow)
     * @return
     */
    public List<AclBinding> listACLsForDomainOwnedTopics() {
        validateTopicConfig();

        final String id = apiSpec.id();
        final String principle = apiSpec.id();
        return List.of(
                // unrestricted access to public topics - must use User:* and not User:domain-*
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, id + ".public", PatternType.PREFIXED),
                        new AccessControlEntry("User:*", "*", AclOperation.READ, AclPermissionType.ALLOW)),

                // READ, WRITE to owned topics
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, id, PatternType.PREFIXED),
                        new AccessControlEntry(formatPrinciple(principle), "*", AclOperation.READ, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, id, PatternType.PREFIXED),
                        new AccessControlEntry(formatPrinciple(principle), "*", AclOperation.WRITE, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, id, PatternType.PREFIXED),
                        new AccessControlEntry(formatPrinciple(principle), "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW))
                );
    }
    public static String formatPrinciple(final String domainId) {
        return "User:domain-" + domainId;
    }
}
