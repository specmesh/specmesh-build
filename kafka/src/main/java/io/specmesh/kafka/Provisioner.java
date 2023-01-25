package io.specmesh.kafka;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.readString;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicListing;

/**
 * Provisions Kafka and SR resources
 */
public final class Provisioner {

    private static final int REQUEST_TIMEOUT = 10;

    private Provisioner() {
    }

    /**
     * Provision topics in the Kafka cluster.
     *
     * @param adminClient
     *            admin client for the Kafka cluster.
     * @param apiSpec
     *            the api spec.
     * @return number of topics created
     * @throws InterruptedException
     *             on interrupt
     * @throws ExecutionException
     *             on remote API call failure
     * @throws TimeoutException
     *             on timeout
     */
    public static int provisionTopics(final AdminClient adminClient, final KafkaApiSpec apiSpec)
            throws InterruptedException, ExecutionException, TimeoutException {

        final var domainTopics = apiSpec.listDomainOwnedTopics();

        final var existingTopics = adminClient.listTopics().listings().get(REQUEST_TIMEOUT, TimeUnit.SECONDS).stream()
                .map(TopicListing::name).collect(Collectors.toList());

        final var newTopicsToCreate = domainTopics.stream()
                .filter(newTopic -> !existingTopics.contains(newTopic.name())).collect(Collectors.toList());

        adminClient.createTopics(newTopicsToCreate).all().get(REQUEST_TIMEOUT, TimeUnit.SECONDS);
        return newTopicsToCreate.size();
    }

    /**
     * Provision schemas to Schema Registry
     *
     * @param apiSpec
     *            the api spec
     * @param schemaRegistryClient
     *            the client for the schema registry
     * @param baseResourcePath
     *            the path under which external schemas are stored.
     */
    public static void provisionSchemas(final KafkaApiSpec apiSpec, final SchemaRegistryClient schemaRegistryClient,
            final String baseResourcePath) {

        final var domainTopics = apiSpec.listDomainOwnedTopics();

        domainTopics.forEach((topic -> {

            final var schemaInfo = apiSpec.schemaInfoForTopic(topic.name());
            final var schemaRef = schemaInfo.schemaRef();
            final String schemaContent;
            try {
                schemaContent = readString(Paths.get(baseResourcePath + "/" + schemaRef), UTF_8);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            final ParsedSchema someSchema = getSchema(topic.name(), schemaRef, baseResourcePath, schemaContent);

            // register the schema against the topic (subject)
            try {
                schemaRegistryClient.register(topic.name() + "-value", someSchema);
            } catch (IOException | RestClientException e) {
                throw new RuntimeException(e);
            }
        }));
    }

    /**
     * Provision acls in the Kafka cluster
     *
     * @param adminClient
     *            th admin client for the cluster.
     * @param apiSpec
     *            the api spec.
     * @throws InterruptedException
     *             on interrupt
     * @throws ExecutionException
     *             on remote API call failure
     * @throws TimeoutException
     *             on timeout
     */
    public static void provisionAcls(final AdminClient adminClient, final KafkaApiSpec apiSpec)
            throws ExecutionException, InterruptedException, TimeoutException {
        adminClient.createAcls(apiSpec.listACLsForDomainOwnedTopics()).all().get(REQUEST_TIMEOUT, TimeUnit.SECONDS);
    }

    static ParsedSchema getSchema(final String topicName, final String schemaRef, final String path,
            final String content) {

        if (schemaRef.endsWith(".avsc")) {
            return new AvroSchema(content);
        }
        if (schemaRef.endsWith(".yml")) {
            return new JsonSchema(content);
        }
        if (schemaRef.endsWith(".proto")) {
            return new ProtobufSchema(content);
        }
        throw new RuntimeException("Failed to handle topic:" + topicName + " schema: " + path);
    }
}
