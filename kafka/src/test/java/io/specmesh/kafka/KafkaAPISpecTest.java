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
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import io.specmesh.apiparser.model.SchemaInfo;
import io.specmesh.test.TestSpecLoader;
import java.util.List;
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
    public void shouldGenerateACLsSoDomainOwnersCanWrite() {
        final List<AclBinding> acls = API_SPEC.listACLsForDomainOwnedTopics();

        assertThat(acls, hasSize(6));

        assertThat(
                "Protected ACL was not created",
                acls.get(0).toString(),
                is(
                        "(pattern=ResourcePattern(resourceType=TOPIC, "
                                + "name=london.hammersmith.olympia.bigdatalondon._protected.retail.subway.food.purchase, "
                                + "patternType=PREFIXED), entry=(principal=User:.some.other.domain.root, host=*, operation=READ, "
                                + "permissionType=ALLOW))"));
    }

    @Test
    public void shouldGetSchemaInfoForOwnedTopics() {
        final List<NewTopic> newTopics = API_SPEC.listDomainOwnedTopics();
        final SchemaInfo schemaInfo = API_SPEC.schemaInfoForTopic(newTopics.get(0).name());
        assertThat(schemaInfo.schemaIdLocation(), is("header"));
    }
}
