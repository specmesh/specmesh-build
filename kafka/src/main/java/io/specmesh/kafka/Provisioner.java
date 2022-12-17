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

public final class Provisioner {
    private Provisioner() {
    }

    public static final int WAIT = 10;

    public static int provisionTopics(final AdminClient adminClient, final KafkaApiSpec apiSpec)
            throws InterruptedException, ExecutionException, TimeoutException {

        final var domainTopics = apiSpec.listDomainOwnedTopics();

        final var existingTopics = adminClient.listTopics().listings().get(WAIT, TimeUnit.SECONDS).stream()
                .map(TopicListing::name).collect(Collectors.toList());

        final var newTopicsToCreate = domainTopics.stream()
                .filter(newTopic -> !existingTopics.contains(newTopic.name())).collect(Collectors.toList());

        adminClient.createTopics(newTopicsToCreate).all().get(WAIT, TimeUnit.SECONDS);
        return newTopicsToCreate.size();
    }

    /**
     * Still need to - add schema compatibility checks - add schema meta data
     * requirement so crappy schemas cannot be published
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

    public static void provisionAcls(final AdminClient adminClient, final KafkaApiSpec apiSpec)
            throws ExecutionException, InterruptedException, TimeoutException {
        adminClient.createAcls(apiSpec.listACLsForDomainOwnedTopics()).all().get(WAIT, TimeUnit.SECONDS);
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
