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
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.acl.AclBinding;
import org.junit.jupiter.api.Test;

public class KafkaAPISpecTest {

    private static final KafkaApiSpec API_SPEC =
            TestSpecLoader.loadFromClassPath("bigdatalondon-api.yaml");

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
                        "(pattern=ResourcePattern(resourceType=TOPIC,"
                                + " name=london.hammersmith.olympia.bigdatalondon._public,"
                                + " patternType=PREFIXED), entry=(principal=User:*, host=*,"
                                + " operation=READ, permissionType=ALLOW))",
                        "(pattern=ResourcePattern(resourceType=TOPIC,"
                                + " name=london.hammersmith.olympia.bigdatalondon._public,"
                                + " patternType=PREFIXED), entry=(principal=User:*, host=*,"
                                + " operation=DESCRIBE, permissionType=ALLOW))"));
    }

    @Test
    public void shouldGenerateAclToAllowSpecificUsersToConsumeProtectedTopics() {
        final Set<AclBinding> acls = API_SPEC.requiredAcls();

        assertThat(
                acls.stream().map(Object::toString).collect(Collectors.toSet()),
                hasItems(
                        "(pattern=ResourcePattern(resourceType=TOPIC,"
                            + " name=london.hammersmith.olympia.bigdatalondon._protected.retail.subway.food.purchase,"
                            + " patternType=LITERAL),"
                            + " entry=(principal=User:.some.other.domain.root, host=*,"
                            + " operation=READ, permissionType=ALLOW))",
                        "(pattern=ResourcePattern(resourceType=TOPIC,"
                            + " name=london.hammersmith.olympia.bigdatalondon._protected.retail.subway.food.purchase,"
                            + " patternType=LITERAL),"
                            + " entry=(principal=User:.some.other.domain.root, host=*,"
                            + " operation=DESCRIBE, permissionType=ALLOW))"));
    }

    @Test
    public void shouldGenerateAclToAllowSelfControlOfPrivateTopics() {
        final Set<AclBinding> acls = API_SPEC.requiredAcls();

        assertThat(
                acls.stream().map(Object::toString).collect(Collectors.toSet()),
                hasItems(
                        "(pattern=ResourcePattern(resourceType=TOPIC,"
                                + " name=london.hammersmith.olympia.bigdatalondon,"
                                + " patternType=PREFIXED),"
                                + " entry=(principal=User:london.hammersmith.olympia.bigdatalondon,"
                                + " host=*, operation=DESCRIBE, permissionType=ALLOW))",
                        "(pattern=ResourcePattern(resourceType=TOPIC,"
                                + " name=london.hammersmith.olympia.bigdatalondon,"
                                + " patternType=PREFIXED),"
                                + " entry=(principal=User:london.hammersmith.olympia.bigdatalondon,"
                                + " host=*, operation=READ, permissionType=ALLOW))",
                        "(pattern=ResourcePattern(resourceType=TOPIC,"
                                + " name=london.hammersmith.olympia.bigdatalondon,"
                                + " patternType=PREFIXED),"
                                + " entry=(principal=User:london.hammersmith.olympia.bigdatalondon,"
                                + " host=*, operation=WRITE, permissionType=ALLOW))"));
    }

    @Test
    public void shouldGenerateAclsToAllowSelfToUseConsumerGroups() {
        final Set<AclBinding> acls = API_SPEC.requiredAcls();

        assertThat(
                acls.stream().map(Object::toString).collect(Collectors.toSet()),
                hasItems(
                        "(pattern=ResourcePattern(resourceType=GROUP,"
                                + " name=london.hammersmith.olympia.bigdatalondon,"
                                + " patternType=PREFIXED),"
                                + " entry=(principal=User:london.hammersmith.olympia.bigdatalondon,"
                                + " host=*, operation=READ, permissionType=ALLOW))"));
    }

    @Test
    public void shouldGetSchemaInfoForOwnedTopics() {
        final List<NewTopic> newTopics = API_SPEC.listDomainOwnedTopics();
        final SchemaInfo schemaInfo = API_SPEC.schemaInfoForTopic(newTopics.get(0).name());
        assertThat(schemaInfo.schemaIdLocation(), is("header"));
    }
}
