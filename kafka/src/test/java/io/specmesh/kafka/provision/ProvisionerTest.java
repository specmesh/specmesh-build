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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.specmesh.kafka.KafkaApiSpec;
import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@SuppressWarnings("resource")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ProvisionerTest {

    private static final String DOMAIN_ID = "mktx.something";

    @Mock private Provisioner.AdminFactory adminFactory;
    @Mock private Provisioner.SrClientFactory srClientFactory;
    @Mock private Provisioner.SpecLoader specLoader;
    @Mock private Provisioner.TopicProvision topicProvisioner;
    @Mock private Provisioner.SchemaProvision schemaProvisioner;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private KafkaApiSpec spec;

    @Mock private SchemaRegistryClient srClient;
    @Mock private Admin adminClient;
    @Mock private Provisioner.AclProvision aclProvisioner;

    @BeforeEach
    void setUp() {
        when(adminFactory.adminClient(any(), any(), any())).thenReturn(adminClient);
        when(srClientFactory.schemaRegistryClient(any(), any(), any())).thenReturn(srClient);
        when(specLoader.loadFromClassPath(any(), any())).thenReturn(spec);
        when(spec.id()).thenReturn(DOMAIN_ID);
    }

    @Test
    void shouldProvideGetters() {
        // When:
        final var provision = Provisioner.builder().brokerUrl("broker").aclDisabled(false).build();

        // Then:
        assertThat(provision.aclDisabled(), is(false));
        assertThat(provision.brokerUrl(), is("broker"));
    }

    @Test
    void shouldThrowIfBrokerUrlNotProvided() {
        // Given:
        final Provisioner provisioner =
                Provisioner.builder()
                        // Not set: .brokerUrl("something")
                        .schemaRegistryUrl("something")
                        .specPath("something")
                        .build();

        // When:
        final Exception e =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                provisioner.provision(
                                        adminFactory,
                                        srClientFactory,
                                        specLoader,
                                        topicProvisioner,
                                        schemaProvisioner,
                                        aclProvisioner));

        // Then:
        assertThat(e.getMessage(), is("Please set a broker url"));
    }

    @Test
    void shouldThrowIfSchemaRegistryUrlNotProvided() {
        // Given:
        final Provisioner provisioner =
                Provisioner.builder()
                        .brokerUrl("something")
                        // Not set: .schemaRegistryUrl("something")
                        .specPath("something")
                        .build();

        // When:
        final Exception e =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                provisioner.provision(
                                        adminFactory,
                                        srClientFactory,
                                        specLoader,
                                        topicProvisioner,
                                        schemaProvisioner,
                                        aclProvisioner));

        // Then:
        assertThat(e.getMessage(), is("Please set a schema registry url"));
    }

    @Test
    void shouldNotThrowIfSchemaRegistryUrlNotProvidedWhenSchemasAreDisabled() {
        // Given:
        final Provisioner provisioner =
                Provisioner.builder()
                        .brokerUrl("something")
                        // Not set: .schemaRegistryUrl("something")
                        .specPath("something")
                        .srDisabled(true)
                        .build();

        // When:
        provisioner.provision(
                adminFactory,
                srClientFactory,
                specLoader,
                topicProvisioner,
                schemaProvisioner,
                aclProvisioner);

        // Then: did not throw
    }

    @Test
    void shouldThrowIfSpecPathNotProvided() {
        // Given:
        final Provisioner provisioner =
                Provisioner.builder()
                        .brokerUrl("something")
                        .schemaRegistryUrl("something")
                        // Not set: .spec("something")
                        .build();

        // When:
        final Exception e =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                provisioner.provision(
                                        adminFactory,
                                        srClientFactory,
                                        specLoader,
                                        topicProvisioner,
                                        schemaProvisioner,
                                        aclProvisioner));

        // Then:
        assertThat(e.getMessage(), is("Please set the path to the specification file"));
    }

    @Test
    void shouldNotThrowIfSpecPathNotProvidedButApiSpecIs() {
        // Given:
        final KafkaApiSpec explicitSpec =
                mock(KafkaApiSpec.class, withSettings().defaultAnswer(RETURNS_DEEP_STUBS));
        final Provisioner provisioner =
                Provisioner.builder()
                        .brokerUrl("something")
                        .schemaRegistryUrl("something")
                        // Not set: .specPath("something")
                        .apiSpec(explicitSpec)
                        .build();

        // When:
        provisioner.provision(
                adminFactory,
                srClientFactory,
                specLoader,
                topicProvisioner,
                schemaProvisioner,
                aclProvisioner);

        // Then: did not throw, and
        verify(topicProvisioner)
                .provision(anyBoolean(), anyBoolean(), anyDouble(), eq(explicitSpec), any());
        verify(schemaProvisioner)
                .provision(anyBoolean(), anyBoolean(), eq(explicitSpec), any(), any());
        verify(aclProvisioner)
                .provision(anyBoolean(), anyBoolean(), eq(explicitSpec), any(), any());
    }

    @Test
    void shouldWorkWithMinimalConfig() {
        // Given:
        final Provisioner provisioner = minimalBuilder().build();

        // When:
        provisioner.provision(
                adminFactory,
                srClientFactory,
                specLoader,
                topicProvisioner,
                schemaProvisioner,
                aclProvisioner);

        // Then:
        verify(adminFactory).adminClient("kafka-url", null, null);
        verify(specLoader).loadFromClassPath("spec-path", Provisioner.class.getClassLoader());
        verify(srClientFactory).schemaRegistryClient("sr-url", null, null);

        verify(topicProvisioner).provision(false, false, 1.0, spec, adminClient);
        verify(schemaProvisioner).provision(false, false, spec, "", srClient);
        verify(aclProvisioner).provision(false, false, spec, DOMAIN_ID, adminClient);
    }

    @Test
    void shouldUseAuthToCreateAdmin() {
        // Given:
        final Provisioner provisioner = minimalBuilder().username("bob").secret("shhhh!").build();

        // When:
        provisioner.provision(
                adminFactory,
                srClientFactory,
                specLoader,
                topicProvisioner,
                schemaProvisioner,
                aclProvisioner);

        // Then:
        verify(adminFactory).adminClient("kafka-url", "bob", "shhhh!");
    }

    @Test
    void shouldUseAuthToCreateSrClient() {
        // Given:
        final Provisioner provisioner =
                minimalBuilder().srApiKey("bob").srApiSecret("shhhh!").build();

        // When:
        provisioner.provision(
                adminFactory,
                srClientFactory,
                specLoader,
                topicProvisioner,
                schemaProvisioner,
                aclProvisioner);

        // Then:
        verify(srClientFactory).schemaRegistryClient("sr-url", "bob", "shhhh!");
    }

    @Test
    void shouldSupportDisablingAcls() {
        // Given:
        final Provisioner provisioner = minimalBuilder().aclDisabled(true).build();

        // When:
        provisioner.provision(
                adminFactory,
                srClientFactory,
                specLoader,
                topicProvisioner,
                schemaProvisioner,
                aclProvisioner);

        // Then:
        verify(aclProvisioner, never()).provision(anyBoolean(), anyBoolean(), any(), any(), any());
    }

    @Test
    void shouldSupportDisablingSchemas() {
        // Given:
        final Provisioner provisioner = minimalBuilder().srDisabled(true).build();

        // When:
        provisioner.provision(
                adminFactory,
                srClientFactory,
                specLoader,
                topicProvisioner,
                schemaProvisioner,
                aclProvisioner);

        // Then:
        verify(schemaProvisioner, never())
                .provision(anyBoolean(), anyBoolean(), any(), any(), any());
    }

    @Test
    void shouldSupportDryRun() {
        // Given:
        final Provisioner provisioner = minimalBuilder().dryRun(true).build();

        // When:
        provisioner.provision(
                adminFactory,
                srClientFactory,
                specLoader,
                topicProvisioner,
                schemaProvisioner,
                aclProvisioner);

        // Then:
        verify(topicProvisioner).provision(eq(true), anyBoolean(), anyDouble(), any(), any());
        verify(schemaProvisioner).provision(eq(true), anyBoolean(), any(), any(), any());
        verify(aclProvisioner).provision(eq(true), anyBoolean(), any(), any(), any());
    }

    @Test
    void shouldSupportCleaningUnspecific() {
        // Given:
        final Provisioner provisioner = minimalBuilder().cleanUnspecified(true).build();

        // When:
        provisioner.provision(
                adminFactory,
                srClientFactory,
                specLoader,
                topicProvisioner,
                schemaProvisioner,
                aclProvisioner);

        // Then:
        verify(topicProvisioner).provision(anyBoolean(), eq(true), anyDouble(), any(), any());
        verify(schemaProvisioner).provision(anyBoolean(), eq(true), any(), any(), any());
        verify(aclProvisioner).provision(anyBoolean(), eq(true), any(), any(), any());
    }

    @Test
    void shouldSupportCustomSchemaRoot() {
        // Given:
        final Provisioner provisioner = minimalBuilder().schemaPath("schema-path").build();

        // When:
        provisioner.provision(
                adminFactory,
                srClientFactory,
                specLoader,
                topicProvisioner,
                schemaProvisioner,
                aclProvisioner);

        // Then:
        verify(schemaProvisioner)
                .provision(anyBoolean(), anyBoolean(), any(), eq("schema-path"), any());
    }

    @Test
    void shouldSupportUserAlias() {
        // Given:
        final Provisioner provisioner = minimalBuilder().domainUserAlias("bob").build();

        // When:
        provisioner.provision(
                adminFactory,
                srClientFactory,
                specLoader,
                topicProvisioner,
                schemaProvisioner,
                aclProvisioner);

        // Then:
        verify(aclProvisioner).provision(anyBoolean(), anyBoolean(), any(), eq("bob"), any());
    }

    @Test
    void shouldSupportExplicitAdminClient() {
        // Given:
        final Admin userAdmin = mock();
        final Provisioner provisioner = minimalBuilder().adminClient(userAdmin).build();

        // When:
        provisioner.provision(
                adminFactory,
                srClientFactory,
                specLoader,
                topicProvisioner,
                schemaProvisioner,
                aclProvisioner);

        // Then:
        verify(topicProvisioner)
                .provision(anyBoolean(), anyBoolean(), anyDouble(), any(), eq(userAdmin));
        verify(aclProvisioner).provision(anyBoolean(), anyBoolean(), any(), any(), eq(userAdmin));
    }

    @Test
    void shouldCloseAdminClient() {
        // Given:
        final Provisioner provisioner = minimalBuilder().build();

        // When:
        provisioner.provision(
                adminFactory,
                srClientFactory,
                specLoader,
                topicProvisioner,
                schemaProvisioner,
                aclProvisioner);

        // Then:
        verify(adminClient).close();
    }

    @Test
    void shouldNotCloseUserSuppliedAdminClientByDefault() {
        // Given:
        final Admin userAdmin = mock();
        final Provisioner provisioner = minimalBuilder().adminClient(userAdmin).build();

        // When:
        provisioner.provision(
                adminFactory,
                srClientFactory,
                specLoader,
                topicProvisioner,
                schemaProvisioner,
                aclProvisioner);

        // Then:
        verify(userAdmin, never()).close();
    }

    @Test
    void shouldCloseUserSuppliedAdminClient() {
        // Given:
        final Admin userAdmin = mock();
        final Provisioner provisioner =
                minimalBuilder().adminClient(userAdmin).closeAdminClient(true).build();

        // When:
        provisioner.provision(
                adminFactory,
                srClientFactory,
                specLoader,
                topicProvisioner,
                schemaProvisioner,
                aclProvisioner);

        // Then:
        verify(userAdmin).close();
    }

    @Test
    void shouldSupportExplicitSrClient() {
        // Given:
        final SchemaRegistryClient client = mock();
        final Provisioner provisioner = minimalBuilder().schemaRegistryClient(client).build();

        // When:
        provisioner.provision(
                adminFactory,
                srClientFactory,
                specLoader,
                topicProvisioner,
                schemaProvisioner,
                aclProvisioner);

        // Then:
        verify(schemaProvisioner).provision(anyBoolean(), anyBoolean(), any(), any(), eq(client));
    }

    @Test
    void shouldCloseSrClient() throws Exception {
        // Given:
        final Provisioner provisioner = minimalBuilder().build();

        // When:
        provisioner.provision(
                adminFactory,
                srClientFactory,
                specLoader,
                topicProvisioner,
                schemaProvisioner,
                aclProvisioner);

        // Then:
        verify(srClient).close();
    }

    @Test
    void shouldNotCloseUserSuppliedSrClientByDefault() throws Exception {
        // Given:
        final SchemaRegistryClient client = mock();
        final Provisioner provisioner = minimalBuilder().schemaRegistryClient(client).build();

        // When:
        provisioner.provision(
                adminFactory,
                srClientFactory,
                specLoader,
                topicProvisioner,
                schemaProvisioner,
                aclProvisioner);

        // Then:
        verify(client, never()).close();
    }

    @Test
    void shouldCloseUserSuppliedSrClient() throws Exception {
        // Given:
        final SchemaRegistryClient client = mock();
        final Provisioner provisioner =
                minimalBuilder().schemaRegistryClient(client).closeSchemaClient(true).build();

        // When:
        provisioner.provision(
                adminFactory,
                srClientFactory,
                specLoader,
                topicProvisioner,
                schemaProvisioner,
                aclProvisioner);

        // Then:
        verify(client).close();
    }

    @Test
    void shouldPassPartitionCountFactor() {
        // Given:
        final double partitionCountFactor = 0.7;
        final Provisioner provisioner =
                minimalBuilder().partitionCountFactor(partitionCountFactor).build();

        // When:
        provisioner.provision(
                adminFactory,
                srClientFactory,
                specLoader,
                topicProvisioner,
                schemaProvisioner,
                aclProvisioner);

        // Then:
        verify(topicProvisioner)
                .provision(anyBoolean(), anyBoolean(), eq(partitionCountFactor), any(), any());
    }

    private static Provisioner.ProvisionerBuilder minimalBuilder() {
        return Provisioner.builder()
                .brokerUrl("kafka-url")
                .schemaRegistryUrl("sr-url")
                .specPath("spec-path");
    }
}
