// CHECKSTYLE_RULES.OFF: FinalLocalVariable
// CHECKSTYLE_RULES.OFF: FinalParameters
package io.specmesh.kafka;


import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import io.specmesh.apiparser.AsyncApiParser;
import io.specmesh.apiparser.model.ApiSpec;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.acl.AclBinding;
import org.junit.jupiter.api.Test;
public class KafkaAPISpecTest {
    final KafkaApiSpec apiSpec = new KafkaApiSpec(getAPISpecFromResource());
//    private AdminClient adminClient;


    @Test
    public void shouldListAppOwnedTopics() throws Exception {
        List<NewTopic> newTopics = apiSpec.listDomainOwnedTopics();
        assertThat(newTopics.size(), is(2));
        // For adminClient.createTopics()
    }

    @Test
    public void shouldGenerateACLsSoDomainOwnersCanWrite() throws Exception {
        List<AclBinding> acls = apiSpec.listACLsForDomainOwnedTopics();

        assertThat(acls.size(), is(4));

        // For adminClient.createAcls(acls);
    }


    private Properties cloneProperties(Properties adminClientProperties, Map<String, String> entries) {
        Properties results = new Properties();
        results.putAll(adminClientProperties);
        results.putAll(entries);
        return results;
    }



    private ApiSpec getAPISpecFromResource() {
        try {
            return new AsyncApiParser().loadResource(getClass().getClassLoader().getResourceAsStream("bigdatalondon-api.yaml"));
        } catch (Throwable t) {
            throw new RuntimeException("Failed to load test resource", t);
        }
    }


}
