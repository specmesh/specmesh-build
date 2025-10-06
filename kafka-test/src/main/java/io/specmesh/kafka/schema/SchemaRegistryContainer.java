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

package io.specmesh.kafka.schema;

import java.net.MalformedURLException;
import java.net.URL;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/** Test container for the Schema Registry */
public final class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {

    public static final DockerImageName DEFAULT_IMAGE_NAME =
            DockerImageName.parse("confluentinc/cp-schema-registry:7.9.1");

    /** Port the SR will listen on. */
    public static final int SCHEMA_REGISTRY_PORT = 8081;

    /**
     * @param dockerImageName docker image version of schema registry
     */
    public SchemaRegistryContainer(final DockerImageName dockerImageName) {
        super(dockerImageName);
        dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);
        withExposedPorts(SCHEMA_REGISTRY_PORT)
                .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:" + SCHEMA_REGISTRY_PORT);
    }

    @Override
    public SchemaRegistryContainer withNetworkAliases(final String... aliases) {
        super.withNetworkAliases(aliases);
        if (aliases.length > 0) {
            withEnv("SCHEMA_REGISTRY_HOST_NAME", aliases[0]);
        }

        return this;
    }

    /**
     * @return the URL of the SR instance, accessible from the host network.
     */
    public URL hostNetworkUrl() {
        try {
            return new URL("http", getHost(), getMappedPort(SCHEMA_REGISTRY_PORT), "");
        } catch (MalformedURLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected void doStart() {
        ensureHostNameSet();
        super.doStart();
    }

    private void ensureHostNameSet() {
        if (!getEnvMap().containsKey("SCHEMA_REGISTRY_HOST_NAME")) {
            withEnv("SCHEMA_REGISTRY_HOST_NAME", getNetworkAliases().get(0));
        }
    }
}
