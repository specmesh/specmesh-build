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

import com.google.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.specmesh.kafka.Clients;
import io.specmesh.kafka.KafkaApiSpec;
import io.specmesh.kafka.provision.schema.SchemaProvisioner;
import java.util.Optional;
import lombok.Builder;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.admin.Admin;

/** SpecMesh Kafka Provisioner */
@Getter
@Accessors(fluent = true)
@Builder
@SuppressFBWarnings
public final class Provisioner {

    static final int REQUEST_TIMEOUT = 60;

    private Status state;

    @Builder.Default private String brokerUrl = "";
    private boolean srDisabled;
    private boolean aclDisabled;
    @Builder.Default private String schemaRegistryUrl = "";
    private String srApiKey;
    private String srApiSecret;
    private SchemaRegistryClient schemaRegistryClient;
    @Builder.Default private boolean closeSchemaClient = false;
    private String schemaPath;
    @Builder.Default private String specPath = "";
    private KafkaApiSpec apiSpec;
    private String username;
    private String secret;
    private Admin adminClient;
    @Builder.Default private boolean closeAdminClient = false;
    private boolean dryRun;
    private boolean cleanUnspecified;

    public Status provision() {
        return provision(
                Provisioner::provision,
                Clients::adminClient,
                Clients::schemaRegistryClient,
                KafkaApiSpec::loadFromClassPath);
    }

    @VisibleForTesting
    Status provision(
            final ProvisionerMethod method,
            final AdminFactory adminFactory,
            final SrClientFactory srClientFactory,
            final SpecLoader specLoader) {
        try {
            ensureApiSpec(specLoader);
            ensureAdminClient(adminFactory);
            ensureSrClient(srClientFactory);

            final var status =
                    method.provision(
                            !aclDisabled,
                            dryRun,
                            cleanUnspecified,
                            apiSpec,
                            schemaPath,
                            adminClient,
                            srDisabled ? Optional.empty() : Optional.of(schemaRegistryClient));

            System.out.println(status.toString());
            this.state = status;
            return status;
        } finally {
            if (closeAdminClient) {
                try {
                    adminClient.close();
                } catch (Exception e) {
                    e.printStackTrace(System.err);
                }
                adminClient = null;
            }

            if (closeSchemaClient) {
                try {
                    schemaRegistryClient.close();
                } catch (Exception e) {
                    e.printStackTrace(System.err);
                }
                schemaRegistryClient = null;
            }
        }
    }

    private void ensureSrClient(final SrClientFactory srClientFactory) {
        if (srDisabled || schemaRegistryClient != null) {
            return;
        }

        if (schemaRegistryUrl.isBlank()) {
            throw new IllegalStateException("Please set a schema registry url");
        }

        schemaRegistryClient =
                srClientFactory.schemaRegistryClient(schemaRegistryUrl, srApiKey, srApiSecret);
        closeSchemaClient = true;
    }

    private void ensureAdminClient(final AdminFactory adminFactory) {
        if (adminClient != null) {
            return;
        }

        if (brokerUrl.isBlank()) {
            throw new IllegalStateException("Please set a broker url");
        }

        adminClient = adminFactory.adminClient(brokerUrl, username, secret);
        closeAdminClient = true;
    }

    private void ensureApiSpec(final SpecLoader specLoader) {
        if (apiSpec != null) {
            return;
        }

        if (specPath.isBlank()) {
            throw new IllegalStateException("Please set the path to the specification file");
        }

        apiSpec = specLoader.loadFromClassPath(specPath, Provisioner.class.getClassLoader());
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private static Status provision(
            final boolean aclEnabled,
            final boolean dryRun,
            final boolean cleanUnspecified,
            final KafkaApiSpec apiSpec,
            final String schemaResources,
            final Admin adminClient,
            final Optional<SchemaRegistryClient> schemaRegistryClient) {

        apiSpec.apiSpec().validate();

        final var status =
                Status.builder()
                        .topics(
                                TopicProvisioner.provision(
                                        dryRun, cleanUnspecified, apiSpec, adminClient));
        schemaRegistryClient.ifPresent(
                registryClient ->
                        status.schemas(
                                SchemaProvisioner.provision(
                                        dryRun,
                                        cleanUnspecified,
                                        apiSpec,
                                        schemaResources,
                                        registryClient)));
        if (aclEnabled) {
            status.acls(AclProvisioner.provision(dryRun, cleanUnspecified, apiSpec, adminClient));
        }
        return status.build();
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    @VisibleForTesting
    interface ProvisionerMethod {
        Status provision(
                boolean aclEnabled,
                boolean dryRun,
                boolean cleanUnspecified,
                KafkaApiSpec apiSpec,
                String schemaResources,
                Admin adminClient,
                Optional<SchemaRegistryClient> schemaRegistryClient);
    }

    @VisibleForTesting
    interface AdminFactory {
        Admin adminClient(String brokerUrl, String username, String secret);
    }

    @VisibleForTesting
    interface SrClientFactory {
        SchemaRegistryClient schemaRegistryClient(
                String schemaRegistryUrl, String srApiKey, String srApiSecret);
    }

    @VisibleForTesting
    interface SpecLoader {
        KafkaApiSpec loadFromClassPath(String spec, ClassLoader classLoader);
    }
}
