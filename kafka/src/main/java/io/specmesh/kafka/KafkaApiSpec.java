package io.specmesh.kafka;


import io.specmesh.apiparser.model.ApiSpec;
import io.specmesh.apiparser.model.SchemaInfo;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

/**
 * Kafka entity mappings from the AsyncAPISpec
 */
public class KafkaApiSpec {
    private final ApiSpec apiSpec;

    /**
     * KafkaAPISpec
     *
     * @param apiSpec
     *            the api spec
     */
    public KafkaApiSpec(final ApiSpec apiSpec) {
        this.apiSpec = apiSpec;
    }

    /**
     * root id of the spec
     *
     * @return the id of the spec
     */
    public String id() {
        return apiSpec.id();
    }

    /**
     * Used by AdminClient.createTopics (includes support for configs overrides i.e.
     * min.insync.replicas
     *
     * @return the owned topics.
     */
    public List<NewTopic> listDomainOwnedTopics() {

        validateTopicConfig();

        final String id = apiSpec.id();

        return apiSpec.channels().entrySet().stream().filter(e -> e.getKey().startsWith(id))
                .map(e -> new NewTopic(e.getKey(), e.getValue().bindings().kafka().partitions(),
                        (short) e.getValue().bindings().kafka().replicas())
                                .configs(e.getValue().bindings().kafka().configs()))
                .collect(Collectors.toList());
    }

    private void validateTopicConfig() {
        final String id = apiSpec.id();
        apiSpec.channels().forEach((k, v) -> {
            if (k.startsWith(id) && v.publish() != null && (v.bindings() == null || v.bindings().kafka() == null)) {
                throw new IllegalStateException(
                        "Kafka bindings are missing from channel: [" + k + "] Domain owner: [" + id + "]");
            }
        });
    }

    /**
     * Create an ACL for the domain-id principle that allows writing to any topic
     * prefixed with the Id Prevent non ACL'd ones from writing to it (somehow)
     *
     * @return Acl bindings for owned topics
     */
    public List<AclBinding> listACLsForDomainOwnedTopics() {
        validateTopicConfig();

        final String id = apiSpec.id();
        final String principle = apiSpec.id();

        final List<Map.Entry<String, String>> grantAccessTo = apiSpec.channels().entrySet().stream()
                .filter(e -> e.getKey().startsWith(id + "._protected.")
                        && e.getValue().publish().tags().toString().contains("grant-access:"))
                .flatMap(v -> v.getValue().publish().tags().stream()
                        .filter(tag -> tag.name().startsWith("grant-access:"))
                        .map(item -> new AbstractMap.SimpleImmutableEntry<>(item.name(), v.getKey())))
                .collect(Collectors.toList());

        final List<AclBinding> protectedAccessAcls = grantAccessTo.stream()
                .map(e -> new AclBinding(new ResourcePattern(ResourceType.TOPIC, e.getValue(), PatternType.PREFIXED),
                        new AccessControlEntry("User:" + e.getKey().substring("grant-access:".length()), "*",
                                AclOperation.READ, AclPermissionType.ALLOW))

                ).collect(Collectors.toList());

        protectedAccessAcls.addAll(List.of(
                // unrestricted access to public topics
                new AclBinding(new ResourcePattern(ResourceType.TOPIC, id + ".public", PatternType.PREFIXED),
                        new AccessControlEntry("User:*", "*", AclOperation.READ, AclPermissionType.ALLOW)),

                new AclBinding(new ResourcePattern(ResourceType.TOPIC, id + "._public", PatternType.PREFIXED),
                        new AccessControlEntry("User:*", "*", AclOperation.READ, AclPermissionType.ALLOW)),

                // READ, WRITE to owned topics
                new AclBinding(new ResourcePattern(ResourceType.TOPIC, id, PatternType.PREFIXED),
                        new AccessControlEntry(formatPrinciple(principle), "*", AclOperation.READ,
                                AclPermissionType.ALLOW)),
                new AclBinding(new ResourcePattern(ResourceType.TOPIC, id, PatternType.PREFIXED),
                        new AccessControlEntry(formatPrinciple(principle), "*", AclOperation.WRITE,
                                AclPermissionType.ALLOW)),
                new AclBinding(new ResourcePattern(ResourceType.TOPIC, id, PatternType.PREFIXED),
                        new AccessControlEntry(formatPrinciple(principle), "*", AclOperation.IDEMPOTENT_WRITE,
                                AclPermissionType.ALLOW))));
        return protectedAccessAcls;
    }

    /**
     * Get schema info for the supplied {@code topicName}
     *
     * @param topicName
     *            the name of the topic
     * @return the schema info.
     */
    public SchemaInfo schemaInfoForTopic(final String topicName) {

        final List<NewTopic> myTopics = listDomainOwnedTopics();
        myTopics.stream().filter(topic -> topic.name().equals(topicName)).findFirst()
                .orElseThrow(() -> new IllegalStateException("Could not find 'owned' topic for:" + topicName));

        return apiSpec.channels().get(topicName).publish().schemaInfo();
    }

    /**
     * Format the principle
     *
     * @param domainIdAsUsername
     *            the domain id
     * @return the principle
     */
    public static String formatPrinciple(final String domainIdAsUsername) {
        return "User:" + domainIdAsUsername;
    }
}
