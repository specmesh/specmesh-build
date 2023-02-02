/*
 * Copyright 2023 SpecMesh Contributors (https://github.com/specmesh)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.specmesh.kafka;

import static java.util.Objects.requireNonNull;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclOperation.IDEMPOTENT_WRITE;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.common.resource.ResourceType.GROUP;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;

import io.specmesh.apiparser.model.ApiSpec;
import io.specmesh.apiparser.model.SchemaInfo;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

/** Kafka entity mappings from the AsyncAPISpec */
public class KafkaApiSpec {
    private final ApiSpec apiSpec;

    /**
     * KafkaAPISpec
     *
     * @param apiSpec the api spec
     */
    public KafkaApiSpec(final ApiSpec apiSpec) {
        this.apiSpec = requireNonNull(apiSpec, "apiSpec");
        validateTopicConfig();
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
        return apiSpec.channels().entrySet().stream()
                .filter(e -> e.getKey().startsWith(apiSpec.id()))
                .map(
                        e ->
                                new NewTopic(
                                                e.getKey(),
                                                e.getValue().bindings().kafka().partitions(),
                                                (short) e.getValue().bindings().kafka().replicas())
                                        .configs(e.getValue().bindings().kafka().configs()))
                .collect(Collectors.toList());
    }

    private void validateTopicConfig() {
        final String id = apiSpec.id();
        apiSpec.channels()
                .forEach(
                        (k, v) -> {
                            if (k.startsWith(id)
                                    && v.publish() != null
                                    && (v.bindings() == null || v.bindings().kafka() == null)) {
                                throw new IllegalStateException(
                                        "Kafka bindings are missing from channel: ["
                                                + k
                                                + "] Domain owner: ["
                                                + id
                                                + "]");
                            }
                        });
    }

    /**
     * Create an ACL for the domain-id principle that allows writing to any topic prefixed with the
     * Id Prevent non ACL'd ones from writing to it (somehow)
     *
     * @return Acl bindings for owned topics
     * @deprecated use {@link #requiredAcls()}
     */
    @Deprecated
    public List<AclBinding> listACLsForDomainOwnedTopics() {
        validateTopicConfig();

        final String id = apiSpec.id();
        final String principal = formatPrincipal(apiSpec.id());

        final List<AclBinding> topicAcls =
                apiSpec.channels().entrySet().stream()
                        .filter(
                                e ->
                                        e.getKey().startsWith(id + "._protected.")
                                                && e.getValue()
                                                        .publish()
                                                        .tags()
                                                        .toString()
                                                        .contains("grant-access:"))
                        .flatMap(
                                v ->
                                        v.getValue().publish().tags().stream()
                                                .filter(
                                                        tag ->
                                                                tag.name()
                                                                        .startsWith(
                                                                                "grant-access:"))
                                                .map(
                                                        tag ->
                                                                tag.name()
                                                                        .substring(
                                                                                "grant-access:"
                                                                                        .length()))
                                                .map(
                                                        user ->
                                                                literalAcls(
                                                                        TOPIC,
                                                                        v.getKey(),
                                                                        formatPrincipal(user),
                                                                        DESCRIBE,
                                                                        READ))
                                                .flatMap(Collection::stream))
                        .collect(Collectors.toList());

        // Unrestricted access to all for public topics:
        topicAcls.addAll(prefixedAcls(TOPIC, id + "._public", "User:*", DESCRIBE, READ));
        // Produce & consume owned topics:
        topicAcls.addAll(prefixedAcls(TOPIC, id, principal, DESCRIBE, READ, WRITE));

        topicAcls.addAll(prefixedAcls(TOPIC, id, principal, IDEMPOTENT_WRITE));
        return topicAcls;
    }

    /**
     * Get the set of required ACLs.
     *
     * <p>This includes ACLs for:
     *
     * <ul>
     *   <li>Acls for everyone to consume the spec's public topics
     *   <li>Acls for configured domains to consume the spec's protected topics
     *   <li>Acls for the spec's domain to produce & consume its topics
     *   <li>Acls for the spec's domain to use its consumer groups
     * </ul>
     *
     * @return returns the set of required acls.
     */
    public Set<AclBinding> requiredAcls() {
        final String id = apiSpec.id();
        final String principal = formatPrincipal(apiSpec.id());

        final Set<AclBinding> acls = new HashSet<>();
        acls.addAll(prefixedAcls(GROUP, id, principal, READ));
        acls.addAll(listACLsForDomainOwnedTopics());
        return acls;
    }

    /**
     * Get schema info for the supplied {@code topicName}
     *
     * @param topicName the name of the topic
     * @return the schema info.
     */
    public SchemaInfo schemaInfoForTopic(final String topicName) {
        final List<NewTopic> myTopics = listDomainOwnedTopics();
        myTopics.stream()
                .filter(topic -> topic.name().equals(topicName))
                .findFirst()
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        "Could not find 'owned' topic for:" + topicName));

        return apiSpec.channels().get(topicName).publish().schemaInfo();
    }

    /**
     * Format the principal
     *
     * @param domainIdAsUsername the domain id
     * @return the principal
     * @deprecated use {@link #formatPrincipal}
     */
    @Deprecated
    public static String formatPrinciple(final String domainIdAsUsername) {
        return formatPrincipal(domainIdAsUsername);
    }

    /**
     * Format the principal
     *
     * @param domainIdAsUsername the domain id
     * @return the principal
     */
    public static String formatPrincipal(final String domainIdAsUsername) {
        return "User:" + domainIdAsUsername;
    }

    private static Set<AclBinding> literalAcls(
            final ResourceType resourceType,
            final String resourceName,
            final String principal,
            final AclOperation... operations) {
        return acls(resourceType, resourceName, principal, PatternType.LITERAL, operations);
    }

    private static Set<AclBinding> prefixedAcls(
            final ResourceType resourceType,
            final String resourceName,
            final String principal,
            final AclOperation... operations) {
        return acls(resourceType, resourceName, principal, PatternType.PREFIXED, operations);
    }

    private static Set<AclBinding> acls(
            final ResourceType resourceType,
            final String resourceName,
            final String principal,
            final PatternType type,
            final AclOperation... operations) {
        final ResourcePattern resourcePattern =
                new ResourcePattern(resourceType, resourceName, type);

        return Arrays.stream(operations)
                .map(op -> new AccessControlEntry(principal, "*", op, AclPermissionType.ALLOW))
                .map(ace -> new AclBinding(resourcePattern, ace))
                .collect(Collectors.toSet());
    }
}
