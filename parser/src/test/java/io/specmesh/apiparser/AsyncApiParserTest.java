package io.specmesh.apiparser;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import io.specmesh.apiparser.model.ApiSpec;
import io.specmesh.apiparser.model.Bindings;
import io.specmesh.apiparser.model.Channel;
import java.util.Map;
import java.util.Set;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

public class AsyncApiParserTest {
    final ApiSpec apiSpec = getAPISpecFromResource();

    @Test
    public void shouldReturnRootIdWithDelimiter() {
        assertThat(apiSpec.id(), is("simple.streetlights"));
    }

    @Test
    public void shouldReturnCanonicalChannelNames() {
        final Map<String, Channel> channelsMap = apiSpec.channels();
        assertThat(channelsMap.size(), is(2));
        assertThat("Should have assembled id + channelname", channelsMap.keySet(),
                hasItem("simple.streetlights.public.light.measured"));
        assertThat("Should use absolute path", channelsMap.keySet(),
                hasItem("london.hammersmith.transport.public.tube"));
    }

    @Test
    public void shouldReturnKafkaBindingsForCreation() {
        final Map<String, Channel> channelsMap = apiSpec.channels();
        assertThat(channelsMap.size(), is(2));
        assertThat("Should have assembled id + channelname", channelsMap.keySet(),
                hasItem("simple.streetlights.public.light.measured"));

        final Bindings producerBindings = channelsMap.get("simple.streetlights.public.light.measured").bindings();
        assertThat(producerBindings.kafka().partitions(), is(3));
        assertThat(producerBindings.kafka().replicas(), is(1));
        assertThat(producerBindings.kafka().retention(), is(1));

        assertThat("Should use absolute path", channelsMap.keySet(),
                hasItem("london.hammersmith.transport.public.tube"));

    }

    @Test
    public void shouldReturnOperationTags() {
        final Map<String, Channel> channelsMap = apiSpec.channels();
        assertThat(channelsMap.size(), is(2));
        assertThat("Should have assembled 'id + channelname'", channelsMap.keySet(),
                hasItem("simple.streetlights.public.light.measured"));

        final Bindings producerBindings = channelsMap.get("simple.streetlights.public.light.measured").bindings();
        assertThat(producerBindings.kafka().configs().size(), is(2));
        assertThat(producerBindings.kafka().configs().keySet(), is(Set.of("cleanup.policy", "retention.ms")));
        assertThat(producerBindings.kafka().envs().size(), is(2));

        assertThat(channelsMap.values().iterator().next().publish().message().tags(), Matchers.iterableWithSize(2));
    }

    @Test
    public void shouldReturnProducerMessageSchema() {
        final Map<String, Channel> channelsMap = apiSpec.channels();
        assertThat(channelsMap.size(), is(2));
        assertThat("Should have assembled 'id + channelname'", channelsMap.keySet(),
                hasItem("simple.streetlights.public.light.measured"));

        final Bindings producerBindings = channelsMap.get("simple.streetlights.public.light.measured").bindings();
        assertThat(producerBindings.kafka().configs().size(), is(2));
        assertThat(producerBindings.kafka().configs().keySet(), is(Set.of("cleanup.policy", "retention.ms")));
        assertThat(producerBindings.kafka().envs().size(), is(2));

        assertThat(channelsMap.values().iterator().next().publish().message().tags(), Matchers.iterableWithSize(2));
    }

    private ApiSpec getAPISpecFromResource() {
        try {
            return new AsyncApiParser()
                    .loadResource(getClass().getClassLoader().getResourceAsStream("test-streetlights-simple-api.yaml"));
        } catch (Throwable t) {
            throw new RuntimeException("Failed to load test resource", t);
        }
    }
}
