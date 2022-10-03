package io.specmesh.apiparser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.specmesh.apiparser.model.ApiSpec;

import java.io.IOException;
import java.io.InputStream;

public class AsyncApiParser {

    public final ApiSpec loadResource(final InputStream inputStream) throws IOException {
        if (inputStream == null || inputStream.available() == 0) {
            throw new RuntimeException("Not found");
        }
        return new ObjectMapper(new YAMLFactory()).readValue(inputStream, ApiSpec.class);
    }

}
