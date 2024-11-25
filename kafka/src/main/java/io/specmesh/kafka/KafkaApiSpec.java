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

import static io.specmesh.apiparser.model.ApiSpec.DELIMITER;
import static io.specmesh.apiparser.model.ApiSpec.PRIVATE;
import static io.specmesh.apiparser.model.ApiSpec.PROTECTED;
import static io.specmesh.apiparser.model.ApiSpec.PUBLIC;
import static java.util.Objects.requireNonNull;
import static org.apache.kafka.common.acl.AclOperation.CREATE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclOperation.IDEMPOTENT_WRITE;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.common.resource.Resource.CLUSTER_NAME;
import static org.apache.kafka.common.resource.ResourceType.CLUSTER;
import static org.apache.kafka.common.resource.ResourceType.GROUP;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.apache.kafka.common.resource.ResourceType.TRANSACTIONAL_ID;

import io.specmesh.apiparser.AsyncApiParser;
import io.specmesh.apiparser.model.ApiSpec;
import io.specmesh.apiparser.model.Channel;
import io.specmesh.apiparser.model.Operation;
import io.specmesh.apiparser.model.SchemaInfo;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

/** Kafka entity mappings from the AsyncAPISpec */
public final class KafkaApiSpec {

    private static final String GRANT_ACCESS_TAG = "grant-access:";

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
                .filter(e -> e.getKey().startsWith(id()))
                .map(
                        e ->
                                new NewTopic(
                                                e.getKey(),
                                                e.getValue().bindings().kafka().partitions(),
                                                e.getValue().bindings().kafka().replicas())
                                        .configs(e.getValue().bindings().kafka().configs()))
                .collect(Collectors.toList());
    }

    /**
     * Create an ACL for the domain-id principal that allows writing to any topic prefixed with the
     * `id` and prevent non ACL'd ones from writing to it (deny access when no acl found property
     * must be set)
     *
     * @return Acl bindings for owned topics
     * @deprecated use {@link #requiredAcls()}
     */
    @Deprecated
    public List<AclBinding> listACLsForDomainOwnedTopics() {
        return listACLsForDomainOwnedTopics(id());
    }

    private List<AclBinding> listACLsForDomainOwnedTopics(final String user) {
        validateTopicConfig();

        final List<AclBinding> topicAcls = new ArrayList<>();
        topicAcls.addAll(ownTopicAcls(user));
        topicAcls.addAll(ownTransactionIdsAcls(user));
        topicAcls.addAll(publicTopicAcls());
        topicAcls.addAll(protectedTopicAcls());
        topicAcls.addAll(privateTopicAcls(user));
        topicAcls.addAll(prefixedAcls(CLUSTER, CLUSTER_NAME, principal(user), IDEMPOTENT_WRITE));
        return topicAcls;
    }

    /**
     * Get the set of required ACLs for this domain spec.
     *
     * <p>This includes {@code ALLOW} ACLs for:
     *
     * <ul>
     *   <li>Everyone to consume the spec's public topics
     *   <li>Specifically configured domains to consume the spec's protected topics
     *   <li>The spec's domain to be able to create ad-hoc private topics
     *   <li>The spec's domain to produce and consume its topics
     *   <li>The spec's domain to use its own consumer groups
     *   <li>The spec's domain to use its own transaction ids
     * </ul>
     *
     * @return returns the set of required acls.
     */
    public Set<AclBinding> requiredAcls() {
        return requiredAcls(id());
    }

    /**
     * Get the set of required ACLs for this domain spec, supplying a custom username.
     *
     * <p>Standard convention is to use the domain id for the username. Only use an alternative name
     * if required.
     *
     * <p>This includes {@code ALLOW} ACLs for:
     *
     * <ul>
     *   <li>Everyone to consume the spec's public topics
     *   <li>Specifically configured domains to consume the spec's protected topics
     *   <li>The spec's domain to be able to create ad-hoc private topics
     *   <li>The spec's domain to produce and consume its topics
     *   <li>The spec's domain to use its own consumer groups
     *   <li>The spec's domain to use its own transaction ids
     * </ul>
     *
     * @param userName the username to use as the principal in the acl bindings.
     * @return returns the set of required acls.
     */
    public Set<AclBinding> requiredAcls(final String userName) {
        final Set<AclBinding> acls = new HashSet<>();
        acls.addAll(ownGroupAcls(userName));
        acls.addAll(listACLsForDomainOwnedTopics(userName));
        acls.addAll(grantAccessControlUsingGrantTagOnly());
        return acls;
    }

    /**
     * Get schema info for the supplied {@code topicName}
     *
     * @param topicName the name of the topic
     * @return the schema info.
     */
    public SchemaInfo schemaInfoForTopic(final String topicName) {
        return ownedTopicSchemas(topicName)
                .orElseThrow(() -> new APIException("No schema defined for topic: " + topicName));
    }

    /**
     * Get info about schemas that are conceptually owned by the supplied {@code topicName}.
     *
     * <p>This differs from {@link #topicSchemas} in that it only returned schemas that should be
     * registered when provisioning the topic.
     *
     * @param topicName the name of the topic
     * @return stream of the schema info.
     */
    public Optional<SchemaInfo> ownedTopicSchemas(final String topicName) {
        final Channel channel = apiSpec.channels().get(topicName);
        if (channel == null) {
            throw new APIException("Unknown topic:" + topicName);
        }

        return Optional.ofNullable(channel.publish()).flatMap(Operation::schemaInfo);
    }

    /**
     * Get schema info for the supplied {@code topicName}
     *
     * <p>This differs from {@link #ownedTopicSchemas} in that it returned all known schemas
     * associated with the topic.
     *
     * @param topicName the name of the topic
     * @return stream of the schema info.
     */
    public Stream<SchemaInfo> topicSchemas(final String topicName) {
        final Channel channel = apiSpec.channels().get(topicName);
        if (channel == null) {
            throw new APIException("Unknown topic:" + topicName);
        }

        return Stream.of(channel.publish(), channel.subscribe())
                .filter(Objects::nonNull)
                .map(Operation::schemaInfo)
                .flatMap(Optional::stream);
    }

    private static String formatPrincipal(final String domainIdAsUsername) {
        return domainIdAsUsername.equals(PUBLIC) ? "User:*" : "User:" + domainIdAsUsername;
    }

    private void validateTopicConfig() {
        apiSpec.channels()
                .forEach(
                        (name, channel) -> {
                            if (name.startsWith(id())
                                    && channel.publish() != null
                                    && (channel.bindings() == null
                                            || channel.bindings().kafka() == null)) {
                                throw new IllegalStateException(
                                        "'publish' channels require Kafka bindings for kafka"
                                                + " bindings for topic config (partitions, replicas"
                                                + " etc) and the root channel level.  channel: ["
                                                + name
                                                + "] Domain owner: ["
                                                + id()
                                                + "]");
                            }
                        });
    }

    private String principal(final String user) {
        return formatPrincipal(user);
    }

    private Set<AclBinding> ownGroupAcls(final String user) {
        return prefixedAcls(GROUP, id(), principal(user), READ);
    }

    private Set<AclBinding> ownTopicAcls(final String user) {
        return prefixedAcls(TOPIC, id(), principal(user), DESCRIBE, READ, WRITE);
    }

    private Set<AclBinding> ownTransactionIdsAcls(final String user) {
        return prefixedAcls(TRANSACTIONAL_ID, id(), principal(user), DESCRIBE, WRITE);
    }

    private Set<AclBinding> publicTopicAcls() {
        return prefixedAcls(TOPIC, id() + DELIMITER + PUBLIC, "User:*", DESCRIBE, READ);
    }

    private List<AclBinding> protectedTopicAcls() {
        return apiSpec.channels().entrySet().stream()
                .filter(e -> e.getKey().startsWith(id() + DELIMITER + PROTECTED + DELIMITER))
                .filter(e -> e.getValue().publish().tags().toString().contains(GRANT_ACCESS_TAG))
                .flatMap(
                        e ->
                                e.getValue().publish().tags().stream()
                                        .filter(tag -> tag.name().startsWith(GRANT_ACCESS_TAG))
                                        .map(tag -> tag.name().substring(GRANT_ACCESS_TAG.length()))
                                        .map(
                                                user ->
                                                        literalAcls(
                                                                TOPIC,
                                                                e.getKey(),
                                                                formatPrincipal(user),
                                                                DESCRIBE,
                                                                READ))
                                        .flatMap(Collection::stream))
                .collect(Collectors.toList());
    }

    /**
     * Uses the alternative grant approach - rather than relying on _public, _protected, _private in
     * the path it returns ACLs based upon the `grant-access` notation whereby access is
     * protected/public using the following: - protected/retricted --> grant-acess:domain.something
     * - public --> grant-access:_public
     *
     * @return ACLs according to the grant-access tags
     */
    @SuppressWarnings("checkstyle:BooleanExpressionComplexity")
    private List<AclBinding> grantAccessControlUsingGrantTagOnly() {
        return apiSpec.channels().entrySet().stream()
                .filter(
                        e ->
                                e.getValue().publish() != null
                                        && !isUsingPathPerms(e.getKey())
                                        && e.getValue()
                                                .publish()
                                                .tags()
                                                .toString()
                                                .contains(GRANT_ACCESS_TAG))
                .flatMap(
                        e ->
                                e.getValue().publish().tags().stream()
                                        .filter(tag -> tag.name().startsWith(GRANT_ACCESS_TAG))
                                        .map(tag -> tag.name().substring(GRANT_ACCESS_TAG.length()))
                                        .map(
                                                user ->
                                                        literalAcls(
                                                                TOPIC,
                                                                e.getKey(),
                                                                formatPrincipal(user),
                                                                DESCRIBE,
                                                                READ))
                                        .flatMap(Collection::stream))
                .collect(Collectors.toList());
    }

    /**
     * the path is using public,private explicit based control
     *
     * @param key resource name
     * @return true if it is
     */
    private boolean isUsingPathPerms(final String key) {
        return key.startsWith(id() + DELIMITER + PRIVATE + DELIMITER)
                || key.startsWith(id() + DELIMITER + PROTECTED + DELIMITER)
                || key.startsWith(id() + DELIMITER + PUBLIC + DELIMITER);
    }

    private Set<AclBinding> privateTopicAcls(final String user) {
        return prefixedAcls(TOPIC, id() + DELIMITER + PRIVATE, principal(user), CREATE);
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

    /**
     * loads the spec from the classpath
     *
     * @param spec to load
     * @param classLoader to use
     * @return the loaded spec
     */
    public static KafkaApiSpec loadFromClassPath(final String spec, final ClassLoader classLoader) {
        try (InputStream clis = classLoader.getResourceAsStream(spec)) {
            return new KafkaApiSpec(new AsyncApiParser().loadResource(clis));
        } catch (Exception e) {
            return loadFromFileSystem(spec);
        }
    }

    /**
     * Fallback to FS when classpath is borked
     *
     * @param spec to load
     * @return loaded spec
     */
    public static KafkaApiSpec loadFromFileSystem(final String spec) {
        try (InputStream fis = new FileInputStream(spec)) {
            return new KafkaApiSpec(new AsyncApiParser().loadResource(fis));
        } catch (Exception ex) {
            throw new APIException("Failed to load spec:" + spec, ex);
        }
    }

    /**
     * Load from string
     *
     * @param spec the contents of the spec.
     * @return loaded spec
     */
    public static KafkaApiSpec loadFromString(final String spec) {
        try {
            return new KafkaApiSpec(new AsyncApiParser().loadResource(spec));
        } catch (Exception ex) {
            throw new APIException("Failed to load spec:" + spec, ex);
        }
    }

    public ApiSpec apiSpec() {
        return apiSpec;
    }

    private static class APIException extends RuntimeException {
        APIException(final String message, final Exception cause) {
            super(message, cause);
        }

        APIException(final String message) {
            super(message);
        }
    }
}
