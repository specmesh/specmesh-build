package io.specmesh.kafka;


import io.specmesh.apiparser.AsyncApiParser;
import io.specmesh.apiparser.model.ApiSpec;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class KafkaAPISpecTest {
    public static final String DOMAIN_ROOT = "simple.streetlights";
    public static final String PUBLIC_LIGHT_MEASURED = ".public.light.measured";
    public static final String PRIVATE_LIGHT_EVENTS = ".private.light.events";
    public static final String FOREIGN_DOMAIN = "london.hammersmith.transport";
    final KafkaApiSpec apiSpec = new KafkaApiSpec(getAPISpecFromResource());
    private AdminClient adminClient;


    @Test
    public void shouldListAppOwnedTopics() throws Exception {
        List<NewTopic> newTopics = apiSpec.listDomainOwnedTopics();
        assertThat(newTopics.size(), is(2));


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
