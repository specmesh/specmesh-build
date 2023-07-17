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

import io.specmesh.apiparser.model.ApiSpec;
import io.specmesh.test.TestSpecLoader;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.acl.AclBinding;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Support custom 'ACL' keyword configurations for `public`, `private` and `protected` */
public class CustomAclKafkaAPISpecTest {
    private KafkaApiSpec kafkaApiSpec;

    @BeforeEach
    public void setUp() {
        // store original property value
        System.setProperty(ApiSpec.SPECMESH_PROTECTED, "zzprotected");
        System.setProperty(ApiSpec.SPECMESH_PUBLIC, "zzpublic");
        System.setProperty(ApiSpec.SPECMESH_PRIVATE, "zzprivate");
        kafkaApiSpec = TestSpecLoader.loadFromClassPath("customacl-bigdatalondon-api.yaml");
    }

    private enum ExpectedAcl {
        READ_PUBLIC_TOPICS(
                "(pattern=ResourcePattern(resourceType=TOPIC,"
                        + " name=london.hammersmith.olympia.bigdatalondon.zzpublic,"
                        + " patternType=PREFIXED), entry=(principal=User:*, host=*,"
                        + " operation=READ, permissionType=ALLOW))"),
        DESCRIBE_PUBLIC_TOPICS(
                "(pattern=ResourcePattern(resourceType=TOPIC,"
                        + " name=london.hammersmith.olympia.bigdatalondon.zzpublic,"
                        + " patternType=PREFIXED), entry=(principal=User:*, host=*,"
                        + " operation=DESCRIBE, permissionType=ALLOW))"),
        READ_PROTECTED_TOPICS(
                "(pattern=ResourcePattern(resourceType=TOPIC,"
                    + " name=london.hammersmith.olympia.bigdatalondon.zzprotected.retail.subway.food.purchase,"
                    + " patternType=LITERAL), entry=(principal=User:some.other.domain.root, host=*,"
                    + " operation=READ, permissionType=ALLOW))"),
        DESCRIBE_PROTECTED_TOPICS(
                "(pattern=ResourcePattern(resourceType=TOPIC,"
                    + " name=london.hammersmith.olympia.bigdatalondon.zzprotected.retail.subway.food.purchase,"
                    + " patternType=LITERAL), entry=(principal=User:some.other.domain.root, host=*,"
                    + " operation=DESCRIBE, permissionType=ALLOW))"),

        CREATE_OWN_PRIVATE_TOPICS(
                "(pattern=ResourcePattern(resourceType=TOPIC,"
                        + " name=london.hammersmith.olympia.bigdatalondon.zzprivate,"
                        + " patternType=PREFIXED),"
                        + " entry=(principal=User:london.hammersmith.olympia.bigdatalondon,"
                        + " host=*, operation=CREATE, permissionType=ALLOW))");

        final String text;

        ExpectedAcl(final String text) {
            this.text = text;
        }
    }

    @Test
    public void shouldGenerateAclToAllowAnyOneToConsumePublicTopics() {
        final Set<AclBinding> acls = kafkaApiSpec.requiredAcls();

        assertThat(
                acls.stream().map(Object::toString).collect(Collectors.toSet()),
                hasItems(
                        ExpectedAcl.READ_PUBLIC_TOPICS.text,
                        ExpectedAcl.DESCRIBE_PUBLIC_TOPICS.text));
    }

    @Test
    public void shouldGenerateAclToAllowSpecificUsersToConsumeProtectedTopics() {
        final Set<AclBinding> acls = kafkaApiSpec.requiredAcls();

        assertThat(
                acls.stream().map(Object::toString).collect(Collectors.toSet()),
                hasItems(
                        ExpectedAcl.READ_PROTECTED_TOPICS.text,
                        ExpectedAcl.DESCRIBE_PROTECTED_TOPICS.text));
    }

    @Test
    public void shouldGenerateAclToAllowControlOfOwnPrivateTopics() {
        final Set<AclBinding> acls = kafkaApiSpec.requiredAcls();

        assertThat(
                acls.stream().map(Object::toString).collect(Collectors.toSet()),
                hasItems(ExpectedAcl.CREATE_OWN_PRIVATE_TOPICS.text));
    }
}
