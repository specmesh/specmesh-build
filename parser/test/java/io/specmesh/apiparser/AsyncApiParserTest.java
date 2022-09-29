package io.specmesh.apiparser;

import io.specmesh.apiparser.model.ApiSpec;
import org.junit.jupiter.api.Test;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;

public class AsyncApiParserTest {

    @Test
    public void shouldDoBasicAssertionOfIdAndChannelCount() throws IOException {

        final ApiSpec apiSpec = new AsyncApiParser().loadResource(getClass().getClassLoader().getResourceAsStream("streetlights-simple-api.yaml"));
        assertThat(apiSpec.getId(), is("urn:simple:streetlights"));
        assertThat(apiSpec.channels().size(), is(2));

//        assertThat()
    }
}
