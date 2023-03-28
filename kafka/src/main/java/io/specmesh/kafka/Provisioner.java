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

import static io.specmesh.kafka.ProvisionStatus.STATE.CREATE;
import static io.specmesh.kafka.ProvisionStatus.STATE.CREATED;
import static io.specmesh.kafka.ProvisionStatus.STATE.FAILED;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.readString;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.specmesh.kafka.ProvisionStatus.SchemaStatus;
import io.specmesh.kafka.ProvisionStatus.Schemas;
import io.specmesh.kafka.ProvisionStatus.Topics;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
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
     * Provision Topics, ACLS and schemas
     *
     * @param validateMode test or execute
     * @param apiSpec given spec
     * @param schemaResources schema path
     * @param adminClient kafka admin client
     * @param schemaRegistryClient sr client
     * @return status of provisioning
     * @throws ProvisioningException when cant provision resources
     */
    public static ProvisionStatus provision(
            final boolean validateMode,
            final KafkaApiSpec apiSpec,
            final String schemaResources,
            final Admin adminClient,
            final Optional<SchemaRegistryClient> schemaRegistryClient) {
        final var status =
                ProvisionStatus.builder()
                        .topics(provisionTopics(validateMode, apiSpec, adminClient));
        schemaRegistryClient.ifPresent(
                registryClient ->
                        status.schemas(
                                provisionSchemas(
                                        validateMode, apiSpec, schemaResources, registryClient)));
        status.acls(provisionAcls(validateMode, apiSpec, adminClient));
        return status.build();
    }

    /**
     * Provision topics in the Kafka cluster.
     *
     * @param validateMode test or execute
     * @param apiSpec the api spec.
     * @param adminClient admin client for the Kafka cluster.
     * @return number of topics created
     * @throws ProvisioningException on provision failure
     */
    public static Topics provisionTopics(
            final boolean validateMode, final KafkaApiSpec apiSpec, final Admin adminClient) {

        final var status = Topics.builder();
        try {

            final var domainTopics = apiSpec.listDomainOwnedTopics();

            status.domainTopics(domainTopics);

            final List<String> existing = existingTopics(adminClient);

            status.existingTopics(existing);

            final var create = creatingTopics(domainTopics, existing);

            status.topicsToCreate(create);

            if (!validateMode) {
                adminClient.createTopics(create).all().get(REQUEST_TIMEOUT, TimeUnit.SECONDS);
                status.topicsCreated(create);
            }
        } catch (InterruptedException | ExecutionException | TimeoutException ex) {
            status.exception(new ProvisioningException("Failed to create topics", ex));
        }
        return status.build();
    }

    /**
     * Create topics
     *
     * @param domainTopics - domain owned
     * @param existingTopics - existing
     * @return list of new topics
     */
    private static List<NewTopic> creatingTopics(
            final List<NewTopic> domainTopics, final List<String> existingTopics) {
        return domainTopics.stream()
                .filter(newTopic -> !existingTopics.contains(newTopic.name()))
                .collect(Collectors.toList());
    }

    /**
     * get existing topics for this domain
     *
     * @param adminClient - connection
     * @return set of existing
     */
    private static List<String> existingTopics(final Admin adminClient) {
        try {
            return adminClient
                    .listTopics()
                    .listings()
                    .get(REQUEST_TIMEOUT, TimeUnit.SECONDS)
                    .stream()
                    .map(TopicListing::name)
                    .collect(Collectors.toList());
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new ProvisioningException("Failed to list topics", e);
        }
    }

    /**
     * Provision schemas to Schema Registry
     *
     * @param validateMode for mode of operation
     * @param apiSpec the api spec
     * @param baseResourcePath the path under which external schemas are stored.
     * @param schemaRegistryClient the client for the schema registry
     * @return status of actions
     */
    public static Schemas provisionSchemas(
            final boolean validateMode,
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
                        final var id =
                                registerSchema(
                                        validateMode,
                                        baseResourcePath,
                                        schemaRegistryClient,
                                        topic,
                                        schemaSubject,
                                        schemaRef,
                                        schemaPath,
                                        readSchemaContent(schemaPath));
                        status.id(id);
                        if (id == 0) {
                            status.state(CREATE);
                        } else {
                            status.state(CREATED);
                        }

                    } catch (ProvisioningException ex) {
                        status.state(FAILED);
                        status.exception(ex);
                    }
                    statusList.add(status.build());
                }));
        return Schemas.builder().schemas(statusList).build();
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    private static int registerSchema(
            final boolean validateMode,
            final String baseResourcePath,
            final SchemaRegistryClient schemaRegistryClient,
            final NewTopic topic,
            final String schemaSubject,
            final String schemaRef,
            final Path schemaPath,
            final String schemaContent) {
        try {
            final var parsedSchema =
                    getSchema(topic.name(), schemaRef, baseResourcePath, schemaContent);
            if (!validateMode) {
                return schemaRegistryClient.register(schemaSubject, parsedSchema);
            } else {
                return 0;
            }
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
     * @param validateMode for mode of operation
     * @param apiSpec the api spec.
     * @param adminClient th admin client for the cluster.
     * @return status of provisioning
     * @throws ProvisioningException on interrupt
     */
    public static ProvisionStatus.Acls provisionAcls(
            final boolean validateMode, final KafkaApiSpec apiSpec, final Admin adminClient) {
        final var aclStatus = ProvisionStatus.Acls.builder();
        try {
            final Set<AclBinding> allAcls = apiSpec.requiredAcls();
            aclStatus.aclsToCreate(allAcls);
            if (!validateMode) {
                adminClient.createAcls(allAcls).all().get(REQUEST_TIMEOUT, TimeUnit.SECONDS);
                aclStatus.aclsCreated(allAcls);
            }
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
}
