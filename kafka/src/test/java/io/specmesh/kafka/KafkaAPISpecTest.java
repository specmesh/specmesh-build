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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import io.specmesh.apiparser.model.SchemaInfo;
import io.specmesh.test.TestSpecLoader;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.acl.AclBinding;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class KafkaAPISpecTest {

    private static final KafkaApiSpec API_SPEC =
            TestSpecLoader.loadFromClassPath("bigdatalondon-api.yaml");

    enum ExpectedAcl {
        READ_PUBLIC_TOPICS(
                "(pattern=ResourcePattern(resourceType=TOPIC,"
                        + " name=london.hammersmith.olympia.bigdatalondon._public,"
                        + " patternType=PREFIXED), entry=(principal=User:*, host=*,"
                        + " operation=READ, permissionType=ALLOW))"),
        DESCRIBE_PUBLIC_TOPICS(
                "(pattern=ResourcePattern(resourceType=TOPIC,"
                        + " name=london.hammersmith.olympia.bigdatalondon._public,"
                        + " patternType=PREFIXED), entry=(principal=User:*, host=*,"
                        + " operation=DESCRIBE, permissionType=ALLOW))"),
        READ_PROTECTED_TOPICS(
                "(pattern=ResourcePattern(resourceType=TOPIC,"
                    + " name=london.hammersmith.olympia.bigdatalondon._protected.retail.subway.food.purchase,"
                    + " patternType=LITERAL), entry=(principal=User:some.other.domain.root, host=*,"
                    + " operation=READ, permissionType=ALLOW))"),
        DESCRIBE_PROTECTED_TOPICS(
                "(pattern=ResourcePattern(resourceType=TOPIC,"
                    + " name=london.hammersmith.olympia.bigdatalondon._protected.retail.subway.food.purchase,"
                    + " patternType=LITERAL), entry=(principal=User:some.other.domain.root, host=*,"
                    + " operation=DESCRIBE, permissionType=ALLOW))"),
        DESCRIBE_OWN_TOPICS(
                "(pattern=ResourcePattern(resourceType=TOPIC,"
                        + " name=london.hammersmith.olympia.bigdatalondon,"
                        + " patternType=PREFIXED),"
                        + " entry=(principal=User:london.hammersmith.olympia.bigdatalondon,"
                        + " host=*, operation=DESCRIBE, permissionType=ALLOW))"),
        READ_OWN_TOPICS(
                "(pattern=ResourcePattern(resourceType=TOPIC,"
                        + " name=london.hammersmith.olympia.bigdatalondon,"
                        + " patternType=PREFIXED),"
                        + " entry=(principal=User:london.hammersmith.olympia.bigdatalondon,"
                        + " host=*, operation=READ, permissionType=ALLOW))"),
        WRITE_OWN_TOPICS(
                "(pattern=ResourcePattern(resourceType=TOPIC,"
                        + " name=london.hammersmith.olympia.bigdatalondon,"
                        + " patternType=PREFIXED),"
                        + " entry=(principal=User:london.hammersmith.olympia.bigdatalondon,"
                        + " host=*, operation=WRITE, permissionType=ALLOW))"),
        CREATE_OWN_PRIVATE_TOPICS(
                "(pattern=ResourcePattern(resourceType=TOPIC,"
                        + " name=london.hammersmith.olympia.bigdatalondon._private,"
                        + " patternType=PREFIXED),"
                        + " entry=(principal=User:london.hammersmith.olympia.bigdatalondon,"
                        + " host=*, operation=CREATE, permissionType=ALLOW))"),
        READ_OWN_GROUPS(
                "(pattern=ResourcePattern(resourceType=GROUP,"
                        + " name=london.hammersmith.olympia.bigdatalondon,"
                        + " patternType=PREFIXED),"
                        + " entry=(principal=User:london.hammersmith.olympia.bigdatalondon,"
                        + " host=*, operation=READ, permissionType=ALLOW))"),
        WRITE_OWN_TX_IDS(
                "(pattern=ResourcePattern(resourceType=TRANSACTIONAL_ID,"
                        + " name=london.hammersmith.olympia.bigdatalondon, patternType=PREFIXED),"
                        + " entry=(principal=User:london.hammersmith.olympia.bigdatalondon, host=*,"
                        + " operation=WRITE, permissionType=ALLOW))"),
        DESCRIBE_OWN_TX_IDS(
                "(pattern=ResourcePattern(resourceType=TRANSACTIONAL_ID,"
                        + " name=london.hammersmith.olympia.bigdatalondon, patternType=PREFIXED),"
                        + " entry=(principal=User:london.hammersmith.olympia.bigdatalondon, host=*,"
                        + " operation=DESCRIBE, permissionType=ALLOW))"),
        OWN_IDEMPOTENT_WRITE(
                "(pattern=ResourcePattern(resourceType=CLUSTER, name=kafka-cluster,"
                        + " patternType=PREFIXED),"
                        + " entry=(principal=User:london.hammersmith.olympia.bigdatalondon, host=*,"
                        + " operation=IDEMPOTENT_WRITE, permissionType=ALLOW))");

        final String text;

        ExpectedAcl(final String text) {
            this.text = text;
        }
    }

    @Test
    public void shouldListAppOwnedTopics() {
        final List<NewTopic> newTopics = API_SPEC.listDomainOwnedTopics();
        assertThat(newTopics, hasSize(3));
    }

    @ParameterizedTest
    @ValueSource(strings = {"bob", ""})
    public void shouldGenerateAclToAllowAnyOneToConsumePublicTopics(final String username) {
        final Set<AclBinding> acls =
                username.isBlank() ? API_SPEC.requiredAcls() : API_SPEC.requiredAcls(username);

        assertThat(
                acls.stream().map(Object::toString).collect(Collectors.toSet()),
                hasItems(
                        ExpectedAcl.READ_PUBLIC_TOPICS.text,
                        ExpectedAcl.DESCRIBE_PUBLIC_TOPICS.text));
    }

    @ParameterizedTest
    @ValueSource(strings = {"bob", ""})
    public void shouldGenerateAclToAllowSpecificUsersToConsumeProtectedTopics(
            final String username) {
        final Set<AclBinding> acls =
                username.isBlank() ? API_SPEC.requiredAcls() : API_SPEC.requiredAcls(username);

        assertThat(
                acls.stream().map(Object::toString).collect(Collectors.toSet()),
                hasItems(
                        ExpectedAcl.READ_PROTECTED_TOPICS.text,
                        ExpectedAcl.DESCRIBE_PROTECTED_TOPICS.text));
    }

    @ParameterizedTest
    @ValueSource(strings = {"bob", ""})
    public void shouldGenerateAclToAllowControlOfOwnPrivateTopics(final String username) {
        final Set<AclBinding> acls =
                username.isBlank() ? API_SPEC.requiredAcls() : API_SPEC.requiredAcls(username);

        assertThat(
                acls.stream().map(Object::toString).collect(Collectors.toSet()),
                hasItems(
                        adjustAcl(ExpectedAcl.DESCRIBE_OWN_TOPICS.text, username),
                        adjustAcl(ExpectedAcl.READ_OWN_TOPICS.text, username),
                        adjustAcl(ExpectedAcl.WRITE_OWN_TOPICS.text, username),
                        adjustAcl(ExpectedAcl.CREATE_OWN_PRIVATE_TOPICS.text, username)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"bob", ""})
    public void shouldGenerateAclsToAllowToUseOwnConsumerGroups(final String username) {
        final Set<AclBinding> acls =
                username.isBlank() ? API_SPEC.requiredAcls() : API_SPEC.requiredAcls(username);

        assertThat(
                acls.stream().map(Object::toString).collect(Collectors.toSet()),
                hasItems(adjustAcl(ExpectedAcl.READ_OWN_GROUPS.text, username)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"bob", ""})
    public void shouldGenerateAclsToAllowToUseOwnTransactionId(final String username) {
        final Set<AclBinding> acls =
                username.isBlank() ? API_SPEC.requiredAcls() : API_SPEC.requiredAcls(username);

        assertThat(
                acls.stream().map(Object::toString).collect(Collectors.toSet()),
                hasItems(
                        adjustAcl(ExpectedAcl.WRITE_OWN_TX_IDS.text, username),
                        adjustAcl(ExpectedAcl.DESCRIBE_OWN_TX_IDS.text, username)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"bob", ""})
    public void shouldGenerateAclsToAllowIdempotentWriteOnOlderClusters(final String username) {
        final Set<AclBinding> acls =
                username.isBlank() ? API_SPEC.requiredAcls() : API_SPEC.requiredAcls(username);

        assertThat(
                acls.stream().map(Object::toString).collect(Collectors.toSet()),
                hasItems(adjustAcl(ExpectedAcl.OWN_IDEMPOTENT_WRITE.text, username)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"bob", ""})
    void shouldNotHaveAnyAdditionalAcls(final String username) {
        final Set<AclBinding> acls =
                username.isBlank() ? API_SPEC.requiredAcls() : API_SPEC.requiredAcls(username);

        assertThat(acls, hasSize(ExpectedAcl.values().length));
    }

    @Test
    public void shouldGetSchemaInfoForOwnedTopics() {
        final Optional<SchemaInfo> schema =
                API_SPEC.ownedTopicSchemas(
                        "london.hammersmith.olympia.bigdatalondon._public.attendee");
        assertThat(schema.flatMap(SchemaInfo::schemaIdLocation), is(Optional.of("header")));
    }

    private String adjustAcl(final String acl, final String username) {
        return username.isBlank()
                ? acl
                : acl.replaceAll("User:" + API_SPEC.id(), "User:" + username);
    }
}
