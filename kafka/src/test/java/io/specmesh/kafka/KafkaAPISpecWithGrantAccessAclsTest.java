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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import io.specmesh.apiparser.model.SchemaInfo;
import io.specmesh.test.TestSpecLoader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.acl.AclBinding;
import org.junit.jupiter.api.Test;

public class KafkaAPISpecWithGrantAccessAclsTest {

    private static final KafkaApiSpec API_SPEC =
            TestSpecLoader.loadFromClassPath("bigdatalondon-api-with-grant-access-acls.yaml");

    private enum ExpectedAcl {
        READ_PUBLIC_TOPICS(
                "(pattern=ResourcePattern(resourceType=TOPIC,"
                        + " name=london.hammersmith.olympia.bigdatalondon.attendee,"
                        + " patternType=LITERAL), entry=(principal=User:*, host=*,"
                        + " operation=READ, permissionType=ALLOW))"),
        DESCRIBE_PUBLIC_TOPICS(
                "(pattern=ResourcePattern(resourceType=TOPIC,"
                        + " name=london.hammersmith.olympia.bigdatalondon.attendee,"
                        + " patternType=LITERAL), entry=(principal=User:*, host=*,"
                        + " operation=DESCRIBE, permissionType=ALLOW))"),
        READ_PROTECTED_TOPICS(
                "(pattern=ResourcePattern(resourceType=TOPIC,"
                    + " name=london.hammersmith.olympia.bigdatalondon.retail.subway.food.purchase,"
                    + " patternType=LITERAL), entry=(principal=User:some.other.domain.root, host=*,"
                    + " operation=READ, permissionType=ALLOW))"),
        DESCRIBE_PROTECTED_TOPICS(
                "(pattern=ResourcePattern(resourceType=TOPIC,"
                    + " name=london.hammersmith.olympia.bigdatalondon.retail.subway.food.purchase,"
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

    @Test
    public void shouldGenerateAclToAllowAnyOneToConsumePublicTopics() {
        final Set<AclBinding> acls = API_SPEC.requiredAcls();

        assertThat(
                acls.stream().map(Object::toString).collect(Collectors.toSet()),
                hasItems(
                        ExpectedAcl.READ_PUBLIC_TOPICS.text,
                        ExpectedAcl.DESCRIBE_PUBLIC_TOPICS.text));
    }

    @Test
    public void shouldGenerateAclToAllowSpecificUsersToConsumeProtectedTopics() {
        final Set<AclBinding> acls = API_SPEC.requiredAcls();

        assertThat(
                acls.stream().map(Object::toString).collect(Collectors.toSet()),
                hasItems(
                        ExpectedAcl.READ_PROTECTED_TOPICS.text,
                        ExpectedAcl.DESCRIBE_PROTECTED_TOPICS.text));
    }

    @Test
    public void shouldGenerateAclToAllowControlOfOwnPrivateTopics() {
        final Set<AclBinding> acls = API_SPEC.requiredAcls();

        assertThat(
                acls.stream().map(Object::toString).collect(Collectors.toSet()),
                hasItems(
                        ExpectedAcl.DESCRIBE_OWN_TOPICS.text,
                        ExpectedAcl.READ_OWN_TOPICS.text,
                        ExpectedAcl.WRITE_OWN_TOPICS.text,
                        ExpectedAcl.CREATE_OWN_PRIVATE_TOPICS.text));
    }

    @Test
    public void shouldGenerateAclsToAllowToUseOwnConsumerGroups() {
        final Set<AclBinding> acls = API_SPEC.requiredAcls();

        assertThat(
                acls.stream().map(Object::toString).collect(Collectors.toSet()),
                hasItems(ExpectedAcl.READ_OWN_GROUPS.text));
    }

    @Test
    public void shouldGenerateAclsToAllowToUseOwnTransactionId() {
        final Set<AclBinding> acls = API_SPEC.requiredAcls();

        assertThat(
                acls.stream().map(Object::toString).collect(Collectors.toSet()),
                hasItems(ExpectedAcl.WRITE_OWN_TX_IDS.text, ExpectedAcl.DESCRIBE_OWN_TX_IDS.text));
    }

    @Test
    public void shouldGenerateAclsToAllowIdempotentWriteOnOlderClusters() {
        final Set<AclBinding> acls = API_SPEC.requiredAcls();

        assertThat(
                acls.stream().map(Object::toString).collect(Collectors.toSet()),
                hasItems(ExpectedAcl.OWN_IDEMPOTENT_WRITE.text));
    }

    @Test
    void shouldNotHaveAnyAdditionalAcls() {
        final Set<String> testAcls =
                new HashSet<>(
                        Arrays.stream(ExpectedAcl.values())
                                .map(e -> e.text)
                                .collect(Collectors.toSet()));

        // need to support `_public` access prefixes
        testAcls.add(KafkaAPISpecTest.ExpectedAcl.READ_PUBLIC_TOPICS.text);
        testAcls.add(KafkaAPISpecTest.ExpectedAcl.DESCRIBE_PUBLIC_TOPICS.text);

        final Set<String> specAcls =
                API_SPEC.requiredAcls().stream().map(Object::toString).collect(Collectors.toSet());
        assertThat(specAcls, containsInAnyOrder(testAcls.toArray()));
    }

    @Test
    public void shouldGetSchemaInfoForOwnedTopics() {
        final Optional<SchemaInfo> schema =
                API_SPEC.ownedTopicSchemas("london.hammersmith.olympia.bigdatalondon.attendee");
        assertThat(schema.flatMap(SchemaInfo::schemaIdLocation), is(Optional.of("header")));
    }
}
