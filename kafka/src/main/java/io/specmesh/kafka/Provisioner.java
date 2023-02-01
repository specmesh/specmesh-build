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

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.acl.AclBinding;

/**
 * Provisions Kafka and SR resources
 */
public final class Provisioner {

    private static final int REQUEST_TIMEOUT = 10;

    private Provisioner() {
    }

    /**
     * Provision topics in the Kafka cluster.
     *
     * @param apiSpec
     *            the api spec.
     * @param adminClient
     *            admin client for the Kafka cluster.
     * @return number of topics created
     * @throws ProvisioningException
     *             on provision failure
     */
    public static int provisionTopics(final KafkaApiSpec apiSpec, final AdminClient adminClient)
            throws ProvisioningException {

        final var domainTopics = apiSpec.listDomainOwnedTopics();

        final List<String> existingTopics;
        try {
            existingTopics = adminClient.listTopics().listings().get(REQUEST_TIMEOUT, TimeUnit.SECONDS).stream()
                    .map(TopicListing::name).collect(Collectors.toList());
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new ProvisioningException(e);
        }

        final var newTopicsToCreate = domainTopics.stream()
                .filter(newTopic -> !existingTopics.contains(newTopic.name())).collect(Collectors.toList());

        try {
            adminClient.createTopics(newTopicsToCreate).all().get(REQUEST_TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new ProvisioningException(e);
        }
        return newTopicsToCreate.size();
    }

    /**
     * Provision schemas to Schema Registry
     *
     * @param apiSpec
     *            the api spec
     * @param baseResourcePath
     *            the path under which external schemas are stored.
     * @param schemaRegistryClient
     *            the client for the schema registry
     */
    public static void provisionSchemas(final KafkaApiSpec apiSpec, final String baseResourcePath,
            final SchemaRegistryClient schemaRegistryClient) {

        final var domainTopics = apiSpec.listDomainOwnedTopics();

        domainTopics.forEach((topic -> {

            final var schemaInfo = apiSpec.schemaInfoForTopic(topic.name());
            final var schemaRef = schemaInfo.schemaRef();
            final String schemaContent;
            try {
                schemaContent = readString(Paths.get(baseResourcePath + "/" + schemaRef), UTF_8);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            final ParsedSchema someSchema = getSchema(topic.name(), schemaRef, baseResourcePath, schemaContent);

            // register the schema against the topic (subject)
            try {
                schemaRegistryClient.register(topic.name() + "-value", someSchema);
            } catch (IOException | RestClientException e) {
                throw new RuntimeException(e);
            }
        }));
    }

    /**
     * Provision acls in the Kafka cluster
     *
     * @param apiSpec
     *            the api spec.
     * @param adminClient
     *            th admin client for the cluster.
     * @throws ProvisioningException
     *             on interrupt
     */
    public static void provisionAcls(final KafkaApiSpec apiSpec, final AdminClient adminClient)
            throws ProvisioningException {
        final List<AclBinding> allAcls = apiSpec.listACLsForDomainOwnedTopics();
        try {
            adminClient.createAcls(allAcls).all().get(REQUEST_TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new ProvisioningException(e);
        }
    }

    static ParsedSchema getSchema(final String topicName, final String schemaRef, final String path,
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
        throw new RuntimeException("Failed to handle topic:" + topicName + " schema: " + path);
    }

    /**
     * Provision Topics, ACLS and schemas
     *
     * @param apiSpec
     *            given spec
     * @param schemaResources
     *            schema path
     * @param adminClient
     *            kafka admin client
     * @param schemaRegistryClient
     *            sr client
     * @throws ProvisioningException
     *             when cant provision resources
     */
    public static void provision(final KafkaApiSpec apiSpec, final String schemaResources,
            final AdminClient adminClient, final SchemaRegistryClient schemaRegistryClient)
            throws ProvisioningException {
        provisionTopics(apiSpec, adminClient);
        provisionSchemas(apiSpec, schemaResources, schemaRegistryClient);
        provisionAcls(apiSpec, adminClient);

    }

    /**
     * setup sasl_plain auth creds
     *
     * @param principle
     *            user name
     * @param secret
     *            secret
     * @return client creds map
     */
    public static Map<String, Object> clientAuthProperties(final String principle, final String secret) {
        return Map.of("sasl.mechanism", "PLAIN", AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT",
                "sasl.jaas.config", String.format("org.apache.kafka.common.security.plain.PlainLoginModule required "
                        + "username=\"%s\" password=\"%s\";", principle, secret));

    }

    /**
     * Configure environment for SASL_PLAIN auth with 2 sets of users
     *
     * @param domainUser
     *            owner user
     * @param domainSecret
     *            their secret
     * @param otherDomainUser
     *            other user
     * @param otherDomainSecret
     *            their secret
     * @param otherConfig
     *            other config needed for the env map
     * @return env map for broker
     *
     */
    public static Map<String, String> testAuthorizerConfig(final String domainUser, final String domainSecret,
            final String otherDomainUser, final String otherDomainSecret, final Map<String, String> otherConfig) {
        final Map<String, String> env = new LinkedHashMap<>();
        // must be 'true' for cluster metadata init - otherwise needs better sec config
        env.put("KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND", "true");
        env.put("KAFKA_AUTHORIZER_CLASS_NAME", "kafka.security.authorizer.AclAuthorizer");
        env.put("KAFKA_SUPER_USERS", "User:OnlySuperUser");
        env.put("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN,SASL_PLAINTEXT");

        env.put("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "BROKER:PLAINTEXT,PLAINTEXT:SASL_PLAINTEXT");
        env.put("KAFKA_LISTENER_NAME_PLAINTEXT_SASL_ENABLED_MECHANISMS", "PLAIN");
        env.put("KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG",
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                        + "username=\"admin\" password=\"admin-secret\" user_admin=\"admin-secret\" "
                        + String.format("user_%s=\"%s\" ", domainUser, domainSecret /* secret */)
                        + String.format("user_%s=\"%s\";", otherDomainUser, otherDomainSecret) /* secret */);

        env.put("KAFKA_SASL_JAAS_CONFIG", "org.apache.kafka.common.security.plain.PlainLoginModule required "
                + "username=\"admin\" " + "password=\"admin-secret\";");
        env.putAll(otherConfig);
        return env;
    }

    /**
     * Provisioning failures
     */
    public static class ProvisioningException extends Exception {

        /**
         * Provisioning failures
         *
         * @param e
         *            exception to cause the failure
         */
        public ProvisioningException(final Exception e) {
            super(e);
        }
    }
}
