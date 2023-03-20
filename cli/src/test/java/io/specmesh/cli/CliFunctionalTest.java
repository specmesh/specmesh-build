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

package io.specmesh.cli;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.specmesh.kafka.DockerKafkaEnvironment;
import io.specmesh.kafka.KafkaEnvironment;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import picocli.CommandLine;

class CliFunctionalTest {

    private static final String OWNER_USER = "simple.schema_demo";

    @RegisterExtension
    private static final KafkaEnvironment KAFKA_ENV =
            DockerKafkaEnvironment.builder()
                    .withSaslAuthentication(
                            "admin", "admin-secret", OWNER_USER, OWNER_USER + "-secret")
                    .withKafkaAcls()
                    .build();

    /**
     * Provision all resources apart from schemas. note: Dont specify SR credentials to avoid schema
     * publishing
     *
     * @throws Exception - when broken
     */
    @Test
    void shouldProvisionTopicsAndAclResourcesWithoutSchemas() throws Exception {

        final KafkaProvisionCommand kafkaProvisionCommand = new KafkaProvisionCommand();

        // Given:
        final CommandLine.ParseResult parseResult =
                new CommandLine(kafkaProvisionCommand)
                        .parseArgs(
                                ("--bootstrap-server "
                                                + KAFKA_ENV.kafkaBootstrapServers()
                                                + " "
                                                + "--spec simple_spec_demo-api.yaml "
                                                + "--username admin "
                                                + "--secret admin-secret ")
                                        .split(" "));

        assertThat(parseResult.matchedArgs().size(), is(4));

        // When:
        kafkaProvisionCommand.run();

        /*
         * NOTE: Loooooose sanity checks follow (they validate 'enough' to ensure the provisioner
         * did 'stuff'
         */

        // Then: check topic resources were created
        final ListTopicsResult topicsResult = KAFKA_ENV.adminClient().listTopics();
        final Set<String> topicNames = topicsResult.names().get();
        assertThat(topicNames.size(), is(4));
        assertThat(
                topicNames,
                is(
                        Set.of(
                                "simple.spec_demo._public.user_signed_up",
                                "simple.spec_demo._private.user_checkout",
                                "simple.spec_demo._protected.purchased",
                                "_schemas")));

        // Then: check ACL resources - _private should be restricted and prefixed
        final ResourcePatternFilter privateAclResource =
                new ResourcePatternFilter(
                        ResourceType.TOPIC, "simple.spec_demo._private", PatternType.PREFIXED);
        assertThat(getAcls(privateAclResource).size(), is(1));

        // Then: check ACL resources - _protected are granted explicitly to tagged domain-ids
        final ResourcePatternFilter protectedAclResource =
                new ResourcePatternFilter(
                        ResourceType.TOPIC,
                        "simple.spec_demo._protected.purchased",
                        PatternType.LITERAL);
        assertThat(getAcls(protectedAclResource).size(), is(2));

        // Then: check ACL resources - _public are granted to y'all
        // should grant access to someone else AND the domain owner
        final ResourcePatternFilter publicAclResource =
                new ResourcePatternFilter(
                        ResourceType.TOPIC, "simple.spec_demo._public", PatternType.PREFIXED);
        assertThat(getAcls(publicAclResource).size(), is(2));
    }

    private Collection<AclBinding> getAcls(final ResourcePatternFilter resourcePatternFilter)
            throws ExecutionException, InterruptedException {
        return KAFKA_ENV
                .adminClient()
                .describeAcls(
                        new AclBindingFilter(resourcePatternFilter, AccessControlEntryFilter.ANY))
                .values()
                .get();
    }
}
