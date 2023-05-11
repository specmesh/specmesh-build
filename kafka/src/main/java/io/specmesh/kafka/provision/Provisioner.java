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
import java.util.Optional;
import org.apache.kafka.clients.admin.Admin;

/** Provisions Kafka and SR resources */
public final class Provisioner {

    public static final int REQUEST_TIMEOUT = 60;

    private Provisioner() {}

    /**
     * Provision Topics, ACLS and schemas
     *
     * @param dryRun test or execute
     * @param apiSpec given spec
     * @param schemaResources schema path
     * @param adminClient kafka admin client
     * @param schemaRegistryClient sr client
     * @return status of provisioning
     * @throws ProvisioningException when cant provision resources
     */
    public static Status provision(
            final boolean dryRun,
            final KafkaApiSpec apiSpec,
            final String schemaResources,
            final Admin adminClient,
            final Optional<SchemaRegistryClient> schemaRegistryClient) {

        apiSpec.apiSpec().validate();

        final var status =
                Status.builder().topics(TopicProvisioner.provision(dryRun, apiSpec, adminClient));
        schemaRegistryClient.ifPresent(
                registryClient ->
                        status.schemas(
                                SchemaProvisioner.provision(
                                        dryRun, apiSpec, schemaResources, registryClient)));
        status.acls(AclProvisioner.provision(dryRun, apiSpec, adminClient));
        return status.build();
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
