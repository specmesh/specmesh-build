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

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.specmesh.kafka.KafkaApiSpec;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.security.plain.PlainLoginModule;

/** Provisions Kafka and SR resources */
public final class Provisioner {

    public static final int REQUEST_TIMEOUT = 60;

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
    public static Status provision(
            final boolean validateMode,
            final KafkaApiSpec apiSpec,
            final String schemaResources,
            final Admin adminClient,
            final Optional<SchemaRegistryClient> schemaRegistryClient) {

        final var status =
                Status.builder()
                        .topics(ProvisionTopics.provision(validateMode, apiSpec, adminClient));
        schemaRegistryClient.ifPresent(
                registryClient ->
                        status.schemas(
                                ProvisionSchemas.provision(
                                        validateMode, apiSpec, schemaResources, registryClient)));
        status.acls(ProvisionAcls.provision(validateMode, apiSpec, adminClient));
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

    public static class ProvisioningException extends RuntimeException {

        ProvisioningException(final String msg) {
            super(msg);
        }

        ProvisioningException(final String msg, final Throwable cause) {
            super(msg, cause);
        }
    }
}
