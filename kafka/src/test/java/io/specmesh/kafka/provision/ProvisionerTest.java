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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.specmesh.kafka.KafkaApiSpec;
import java.util.Optional;
import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@SuppressWarnings("resource")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ProvisionerTest {

    @Mock private Provisioner.ProvisionerMethod provisionMethod;
    @Mock private Provisioner.AdminFactory adminFactory;
    @Mock private Provisioner.SrClientFactory srClientFactory;
    @Mock private Provisioner.SpecLoader specLoader;
    @Mock private Status status;
    @Mock private KafkaApiSpec spec;
    @Mock private SchemaRegistryClient srClient;
    @Mock private Admin adminClient;

    @BeforeEach
    void setUp() {
        when(provisionMethod.provision(
                        anyBoolean(), anyBoolean(), anyBoolean(), any(), any(), any(), any()))
                .thenReturn(status);

        when(adminFactory.adminClient(any(), any(), any())).thenReturn(adminClient);
        when(srClientFactory.schemaRegistryClient(any(), any(), any())).thenReturn(srClient);
        when(specLoader.loadFromClassPath(any(), any())).thenReturn(spec);
    }

    @Test
    void shouldProvideGetters() {
        // When:
        final var provision = Provisioner.builder().brokerUrl("borker").aclDisabled(false).build();

        // Then:
        assertThat(provision.aclDisabled(), is(false));
        assertThat(provision.brokerUrl(), is("borker"));
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
                                        provisionMethod,
                                        adminFactory,
                                        srClientFactory,
                                        specLoader));

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
                                        provisionMethod,
                                        adminFactory,
                                        srClientFactory,
                                        specLoader));

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
        provisioner.provision(provisionMethod, adminFactory, srClientFactory, specLoader);

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
                                        provisionMethod,
                                        adminFactory,
                                        srClientFactory,
                                        specLoader));

        // Then:
        assertThat(e.getMessage(), is("Please set the path to the specification file"));
    }

    @Test
    void shouldNotThrowIfSpecPathNotProvidedButApiSpecIs() {
        // Given:
        final KafkaApiSpec explicitSpec = mock(KafkaApiSpec.class);
        final Provisioner provisioner =
                Provisioner.builder()
                        .brokerUrl("something")
                        .schemaRegistryUrl("something")
                        // Not set: .specPath("something")
                        .apiSpec(explicitSpec)
                        .build();

        // When:
        provisioner.provision(provisionMethod, adminFactory, srClientFactory, specLoader);

        // Then: did not throw, and
        verify(provisionMethod)
                .provision(
                        anyBoolean(),
                        anyBoolean(),
                        anyBoolean(),
                        eq(explicitSpec),
                        any(),
                        any(),
                        any());
    }

    @Test
    void shouldWorkWithMinimalConfig() {
        // Given:
        final Provisioner provisioner = minimalBuilder().build();

        // When:
        provisioner.provision(provisionMethod, adminFactory, srClientFactory, specLoader);

        // Then:
        verify(adminFactory).adminClient("kafka-url", null, null);
        verify(specLoader).loadFromClassPath("spec-path", Provisioner.class.getClassLoader());
        verify(srClientFactory).schemaRegistryClient("sr-url", null, null);
        verify(provisionMethod)
                .provision(true, false, false, spec, null, adminClient, Optional.of(srClient));
    }

    @Test
    void shouldUseAuthToCreateAdmin() {
        // Given:
        final Provisioner provisioner = minimalBuilder().username("bob").secret("shhhh!").build();

        // When:
        provisioner.provision(provisionMethod, adminFactory, srClientFactory, specLoader);

        // Then:
        verify(adminFactory).adminClient("kafka-url", "bob", "shhhh!");
    }

    @Test
    void shouldUseAuthToCreateSrClient() {
        // Given:
        final Provisioner provisioner =
                minimalBuilder().srApiKey("bob").srApiSecret("shhhh!").build();

        // When:
        provisioner.provision(provisionMethod, adminFactory, srClientFactory, specLoader);

        // Then:
        verify(srClientFactory).schemaRegistryClient("sr-url", "bob", "shhhh!");
    }

    @Test
    void shouldSupportDisablingAcls() {
        // Given:
        final Provisioner provisioner = minimalBuilder().aclDisabled(true).build();

        // When:
        provisioner.provision(provisionMethod, adminFactory, srClientFactory, specLoader);

        // Then:
        verify(provisionMethod)
                .provision(eq(false), anyBoolean(), anyBoolean(), any(), any(), any(), any());
    }

    @Test
    void shouldSupportDisablingSchemas() {
        // Given:
        final Provisioner provisioner = minimalBuilder().srDisabled(true).build();

        // When:
        provisioner.provision(provisionMethod, adminFactory, srClientFactory, specLoader);

        // Then:
        verify(provisionMethod)
                .provision(
                        anyBoolean(),
                        anyBoolean(),
                        anyBoolean(),
                        any(),
                        any(),
                        any(),
                        eq(Optional.empty()));
    }

    @Test
    void shouldSupportDryRun() {
        // Given:
        final Provisioner provisioner = minimalBuilder().dryRun(true).build();

        // When:
        provisioner.provision(provisionMethod, adminFactory, srClientFactory, specLoader);

        // Then:
        verify(provisionMethod)
                .provision(anyBoolean(), eq(true), anyBoolean(), any(), any(), any(), any());
    }

    @Test
    void shouldSupportCleaningUnspecific() {
        // Given:
        final Provisioner provisioner = minimalBuilder().cleanUnspecified(true).build();

        // When:
        provisioner.provision(provisionMethod, adminFactory, srClientFactory, specLoader);

        // Then:
        verify(provisionMethod)
                .provision(anyBoolean(), anyBoolean(), eq(true), any(), any(), any(), any());
    }

    @Test
    void shouldSupportCustomSchemaRoot() {
        // Given:
        final Provisioner provisioner = minimalBuilder().schemaPath("schema-path").build();

        // When:
        provisioner.provision(provisionMethod, adminFactory, srClientFactory, specLoader);

        // Then:
        verify(provisionMethod)
                .provision(
                        anyBoolean(),
                        anyBoolean(),
                        anyBoolean(),
                        any(),
                        eq("schema-path"),
                        any(),
                        any());
    }

    private static Provisioner.ProvisionerBuilder minimalBuilder() {
        return Provisioner.builder()
                .brokerUrl("kafka-url")
                .schemaRegistryUrl("sr-url")
                .specPath("spec-path");
    }
}
