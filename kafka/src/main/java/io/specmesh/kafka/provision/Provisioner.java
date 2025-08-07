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
import java.util.Collection;
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

    @Builder.Default private String brokerUrl = "";
    private boolean srDisabled;
    private boolean aclDisabled;
    @Builder.Default private String schemaRegistryUrl = "";
    private String srApiKey;
    private String srApiSecret;
    private SchemaRegistryClient schemaRegistryClient;
    @Builder.Default private boolean closeSchemaClient = false;
    @Builder.Default private String schemaPath = "";
    @Builder.Default private String specPath = "";
    @Builder.Default private String domainUserAlias = "";
    private KafkaApiSpec apiSpec;
    private String username;
    private String secret;
    private Admin adminClient;
    @Builder.Default private boolean closeAdminClient = false;
    private boolean dryRun;
    private boolean cleanUnspecified;
    @Builder.Default private double partitionCountFactor = 1.0;

    public Status provision() {
        return provision(
                Clients::adminClient,
                Clients::schemaRegistryClient,
                KafkaApiSpec::loadFromClassPath,
                TopicProvisioner::provision,
                SchemaProvisioner::provision,
                AclProvisioner::provision);
    }

    @VisibleForTesting
    Status provision(
            final AdminFactory adminFactory,
            final SrClientFactory srClientFactory,
            final SpecLoader specLoader,
            final TopicProvision topicProvision,
            final SchemaProvision schemaProvision,
            final AclProvision aclProvision) {
        try {
            ensureApiSpec(specLoader);
            ensureAdminClient(adminFactory);
            ensureSrClient(srClientFactory);

            final Status status = provision(topicProvision, schemaProvision, aclProvision);

            System.out.println(status);
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

    private Status provision(
            final TopicProvision topicProvision,
            final SchemaProvision schemaProvision,
            final AclProvision aclProvision) {
        final String userName = domainUserAlias.isBlank() ? apiSpec.id() : domainUserAlias;

        final Status.StatusBuilder status =
                Status.builder()
                        .topics(
                                topicProvision.provision(
                                        dryRun,
                                        cleanUnspecified,
                                        partitionCountFactor,
                                        apiSpec,
                                        adminClient));
        if (!srDisabled) {
            status.schemas(
                    schemaProvision.provision(
                            dryRun, cleanUnspecified, apiSpec, schemaPath, schemaRegistryClient));
        }

        if (!aclDisabled) {
            status.acls(
                    aclProvision.provision(
                            dryRun, cleanUnspecified, apiSpec, userName, adminClient));
        }
        return status.build();
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

    @VisibleForTesting
    interface TopicProvision {
        Collection<TopicProvisioner.Topic> provision(
                boolean dryRun,
                boolean cleanUnspecified,
                double partitionCountFactor,
                KafkaApiSpec apiSpec,
                Admin adminClient);
    }

    @VisibleForTesting
    interface SchemaProvision {
        Collection<SchemaProvisioner.Schema> provision(
                boolean dryRun,
                boolean cleanUnspecified,
                KafkaApiSpec apiSpec,
                String baseResourcePath,
                SchemaRegistryClient client);
    }

    @VisibleForTesting
    interface AclProvision {
        Collection<AclProvisioner.Acl> provision(
                boolean dryRun,
                boolean cleanUnspecified,
                KafkaApiSpec apiSpec,
                String userName,
                Admin adminClient);
    }
}
