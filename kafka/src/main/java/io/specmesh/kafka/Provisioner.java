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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.readString;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.specmesh.apiparser.model.SchemaInfo;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.security.plain.PlainLoginModule;

/** Provisions Kafka and SR resources */
public final class Provisioner {

    private static final int REQUEST_TIMEOUT = 60;

    private Provisioner() {}

    /**
     * Provision topics in the Kafka cluster.
     *
     * @param apiSpec the api spec.
     * @param adminClient admin client for the Kafka cluster.
     * @return number of topics created
     * @throws ProvisioningException on provision failure
     */
    public static TopicProvisionStatus provisionTopics(
            final KafkaApiSpec apiSpec, final Admin adminClient) {

        final var domainTopics = apiSpec.listDomainOwnedTopics();

        final var status = TopicProvisionStatus.builder().domainTopics(domainTopics);

        final List<String> existingTopics;
        try {
            existingTopics =
                    adminClient
                            .listTopics()
                            .listings()
                            .get(REQUEST_TIMEOUT, TimeUnit.SECONDS)
                            .stream()
                            .map(TopicListing::name)
                            .collect(Collectors.toList());
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new ProvisioningException("Failed to list topics", e);
        }

        status.existingTopics(existingTopics);

        final var newTopicsToCreate =
                domainTopics.stream()
                        .filter(newTopic -> !existingTopics.contains(newTopic.name()))
                        .collect(Collectors.toList());

        status.createTopics(newTopicsToCreate);

        try {
            adminClient
                    .createTopics(newTopicsToCreate)
                    .all()
                    .get(REQUEST_TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new ProvisioningException("Failed to create topics", e);
        }
        return status.build();
    }

    /**
     * Provision schemas to Schema Registry
     *
     * @param apiSpec the api spec
     * @param baseResourcePath the path under which external schemas are stored.
     * @param schemaRegistryClient the client for the schema registry
     * @return status of actions
     */
    public static List<SchemaStatus> provisionSchemas(
            final KafkaApiSpec apiSpec,
            final String baseResourcePath,
            final SchemaRegistryClient schemaRegistryClient) {

        final var domainTopics = apiSpec.listDomainOwnedTopics();

        final var statusList = new ArrayList<SchemaStatus>();

        domainTopics.forEach(
                (topic -> {
                    final var schemaSubject = topic.name() + "-value";
                    final var schemaInfo = apiSpec.schemaInfoForTopic(topic.name());
                    final var schemaRef = schemaInfo.schemaRef();
                    final var schemaPath = Paths.get(baseResourcePath, schemaRef);
                    final var status =
                            SchemaStatus.builder()
                                    .schemaSubject(schemaSubject)
                                    .schemaInfo(schemaInfo)
                                    .schemaPath(schemaPath.toAbsolutePath().toString());
                    try {
                        // register the schema against the topic (subject)
                        registerSchema(
                                baseResourcePath,
                                schemaRegistryClient,
                                topic,
                                schemaSubject,
                                schemaRef,
                                schemaPath,
                                readSchemaContent(schemaPath));
                    } catch (ProvisioningException ex) {
                        status.exception(ex);
                    }
                    statusList.add(status.build());
                }));
        return statusList;
    }

    private static void registerSchema(
            final String baseResourcePath,
            final SchemaRegistryClient schemaRegistryClient,
            final NewTopic topic,
            final String schemaSubject,
            final String schemaRef,
            final Path schemaPath,
            final String schemaContent) {
        try {
            final ParsedSchema someSchema =
                    getSchema(topic.name(), schemaRef, baseResourcePath, schemaContent);

            schemaRegistryClient.register(schemaSubject, someSchema);
        } catch (IOException | RestClientException e) {
            throw new ProvisioningException(
                    "Failed to register schema. topic: " + topic.name() + ", schema:" + schemaPath,
                    e);
        }
    }

    private static String readSchemaContent(final Path schemaPath) {
        final String schemaContent;
        try {
            schemaContent = readString(schemaPath, UTF_8);
        } catch (IOException e) {
            throw new ProvisioningException("Failed to read schema from: " + schemaPath, e);
        }
        return schemaContent;
    }

    /**
     * Provision acls in the Kafka cluster
     *
     * @param apiSpec the api spec.
     * @param adminClient th admin client for the cluster.
     * @return status of provisioning
     * @throws ProvisioningException on interrupt
     */
    public static AclStatus provisionAcls(final KafkaApiSpec apiSpec, final Admin adminClient) {
        final var aclStatus = AclStatus.builder();
        try {
            final Set<AclBinding> allAcls = apiSpec.requiredAcls();
            aclStatus.aclsToCreate(allAcls);
            adminClient.createAcls(allAcls).all().get(REQUEST_TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            aclStatus.exception(new ProvisioningException("Failed to create ACLs", e));
        }
        return aclStatus.build();
    }

    static ParsedSchema getSchema(
            final String topicName,
            final String schemaRef,
            final String path,
            final String content) {

        if (schemaRef.endsWith(".avsc")) {
            return new AvroSchema(content);
        }
        if (schemaRef.endsWith(".yml")) {
            return new JsonSchema(content);
        }
        if (schemaRef.endsWith(".proto")) {
            return new ProtobufSchema(content);
        }
        throw new ProvisioningException(
                "Unsupported schema type for:" + topicName + ", schema: " + path);
    }

    /**
     * Provision Topics, ACLS and schemas
     *
     * @param apiSpec given spec
     * @param schemaResources schema path
     * @param adminClient kafka admin client
     * @param schemaRegistryClient sr client
     * @return status of provisioning
     * @throws ProvisioningException when cant provision resources
     */
    public static Status provision(
            final KafkaApiSpec apiSpec,
            final String schemaResources,
            final Admin adminClient,
            final Optional<SchemaRegistryClient> schemaRegistryClient) {
        final var status = Status.builder().topics(provisionTopics(apiSpec, adminClient));
        schemaRegistryClient.ifPresent(
                registryClient ->
                        status.schemas(provisionSchemas(apiSpec, schemaResources, registryClient)));
        status.acls(provisionAcls(apiSpec, adminClient));
        return status.build();
    }

    /**
     * setup sasl_plain auth creds
     *
     * @param principle user name
     * @param secret secret
     * @return client creds map
     */
    public static Map<String, Object> clientSaslAuthProperties(
            final String principle, final String secret) {
        return Map.of(
                "sasl.mechanism",
                "PLAIN",
                AdminClientConfig.SECURITY_PROTOCOL_CONFIG,
                "SASL_PLAINTEXT",
                "sasl.jaas.config",
                buildJaasConfig(principle, secret));
    }

    private static String buildJaasConfig(final String userName, final String password) {
        return PlainLoginModule.class.getCanonicalName()
                + " required "
                + "username=\""
                + userName
                + "\" password=\""
                + password
                + "\";";
    }

    private static class ProvisioningException extends RuntimeException {

        ProvisioningException(final String msg) {
            super(msg);
        }

        ProvisioningException(final String msg, final Throwable cause) {
            super(msg, cause);
        }
    }

    /** Accumulated Provision status of Underlying resources */
    @Builder
    @Data
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    @Accessors(fluent = true)
    @SuppressFBWarnings
    public static class Status {
        private TopicProvisionStatus topics;
        @Builder.Default private List<SchemaStatus> schemas = Collections.emptyList();
        private AclStatus acls;
    }

    @Builder
    @Data
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    @Accessors(fluent = true)
    @SuppressFBWarnings
    public static class TopicProvisionStatus {

        @Builder.Default private List<NewTopic> domainTopics = Collections.emptyList();
        @Builder.Default private List<String> existingTopics = Collections.emptyList();
        @Builder.Default private List<NewTopic> createTopics = Collections.emptyList();

        private Exception exception;
    }

    @Builder
    @Data
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    @Accessors(fluent = true)
    @SuppressFBWarnings
    public static class SchemaStatus {
        private String schemaSubject;
        private SchemaInfo schemaInfo;
        private String schemaPath;
        private Exception exception;
    }

    @Builder
    @Data
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    @Accessors(fluent = true)
    @SuppressFBWarnings
    public static class AclStatus {
        @Builder.Default private Set<AclBinding> aclsToCreate = Collections.emptySet();
        private Exception exception;
    }
}
