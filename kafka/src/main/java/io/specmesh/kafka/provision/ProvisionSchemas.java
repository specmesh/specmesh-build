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

package io.specmesh.kafka.provision;

import static io.specmesh.kafka.provision.Status.STATE.CREATE;
import static io.specmesh.kafka.provision.Status.STATE.CREATED;
import static io.specmesh.kafka.provision.Status.STATE.FAILED;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.specmesh.kafka.KafkaApiSpec;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import org.apache.kafka.clients.admin.NewTopic;

public final class ProvisionSchemas {

    /** defensive */
    private ProvisionSchemas() {}
    /**
     * Provision schemas to Schema Registry
     *
     * @param validateMode for mode of operation
     * @param apiSpec the api spec
     * @param baseResourcePath the path under which external schemas are stored.
     * @param schemaRegistryClient the client for the schema registry
     * @return status of actions
     */
    public static Status.Schemas provision(
            final boolean validateMode,
            final KafkaApiSpec apiSpec,
            final String baseResourcePath,
            final SchemaRegistryClient schemaRegistryClient) {

        final var domainTopics = apiSpec.listDomainOwnedTopics();

        final var statusList = new ArrayList<Status.SchemaStatus>();

        domainTopics.forEach(
                (topic -> {
                    final var schemaSubject = topic.name() + "-value";
                    final var schemaInfo = apiSpec.schemaInfoForTopic(topic.name());
                    final var schemaRef = schemaInfo.schemaRef();
                    final var schemaPath = Paths.get(baseResourcePath, schemaRef);
                    final var status =
                            Status.SchemaStatus.builder()
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

                    } catch (Provisioner.ProvisioningException ex) {
                        status.state(FAILED);
                        status.exception(ex);
                    }
                    statusList.add(status.build());
                }));
        return Status.Schemas.builder().schemas(statusList).build();
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    static int registerSchema(
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
            throw new Provisioner.ProvisioningException(
                    "Failed to register schema. topic: " + topic.name() + ", schema:" + schemaPath,
                    e);
        }
    }

    static String readSchemaContent(final Path schemaPath) {
        final String schemaContent;
        try {
            schemaContent = Files.readString(schemaPath, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new Provisioner.ProvisioningException(
                    "Failed to read schema from: " + schemaPath, e);
        }
        return schemaContent;
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
        throw new Provisioner.ProvisioningException(
                "Unsupported schema type for:" + topicName + ", schema: " + path);
    }
}
