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

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.specmesh.apiparser.AsyncApiParser;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.security.plain.PlainLoginModule;

public final class Utils {

    private Utils() {}

    /**
     * loads the spec from the classpath
     *
     * @param spec to load
     * @param classLoader to use
     * @return the loaded spec
     */
    public static KafkaApiSpec loadFromClassPath(final String spec, final ClassLoader classLoader) {
        try (InputStream clis = classLoader.getResourceAsStream(spec)) {
            return new KafkaApiSpec(new AsyncApiParser().loadResource(clis));
        } catch (Exception e) {
            return loadFromFileSystem(spec);
        }
    }

    /**
     * Fallback to FS when classpath is borked
     *
     * @param spec to load
     * @return loaded spec
     */
    private static KafkaApiSpec loadFromFileSystem(final String spec) {
        try (InputStream fis = new FileInputStream(spec)) {
            return new KafkaApiSpec(new AsyncApiParser().loadResource(fis));
        } catch (Exception ex) {
            throw new UtilityException("Failed to load spec:" + spec, ex);
        }
    }

    /**
     * AdminClient access
     *
     * @param brokerUrl broker address
     * @param username - user
     * @param secret - secrets
     * @return = adminClient
     */
    public static Admin adminClient(
            final String brokerUrl, final String username, final String secret) {

        try {
            final Map<String, Object> properties = new HashMap<>();
            properties.put(AdminClientConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
            properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);

            if (username != null) {
                properties.putAll(clientSaslAuthProperties(username, secret));
            }

            return AdminClient.create(properties);
        } catch (Exception ex) {
            throw new UtilityException(
                    "cannot load:" + brokerUrl + " with username:" + username, ex);
        }
    }

    /**
     * setup sasl_plain auth creds
     *
     * @param principle user name
     * @param secret secret
     * @return client creds map
     */
    public static Map<String, Object> clientSaslAuthProperties(
            final String principle, final String secret) {
        return Map.of(
                "sasl.mechanism",
                "PLAIN",
                AdminClientConfig.SECURITY_PROTOCOL_CONFIG,
                "SASL_PLAINTEXT",
                "sasl.jaas.config",
                buildJaasConfig(principle, secret));
    }

    private static String buildJaasConfig(final String userName, final String password) {
        return PlainLoginModule.class.getCanonicalName()
                + " required "
                + "username=\""
                + userName
                + "\" password=\""
                + password
                + "\";";
    }

    public static Optional<SchemaRegistryClient> schemaRegistryClient(
            final String schemaRegistryUrl, final String srApiKey, final String srApiSecret) {
        if (schemaRegistryUrl != null) {
            final Map<String, Object> properties = new HashMap<>();
            if (srApiKey != null) {
                properties.put(
                        SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
                properties.put(
                        SchemaRegistryClientConfig.USER_INFO_CONFIG, srApiKey + ":" + srApiSecret);
            }
            return Optional.of(new CachedSchemaRegistryClient(schemaRegistryUrl, 5, properties));
        } else {
            return Optional.empty();
        }
    }

    private static class UtilityException extends RuntimeException {
        UtilityException(final String message, final Exception cause) {
            super(message, cause);
        }

        UtilityException(final String message) {
            super(message);
        }
    }
}
