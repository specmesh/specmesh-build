package io.specmesh.kafka.topic;

import lombok.experimental.UtilityClass;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

/**
 * Topic utility functions used to pull TopicDescriptions and
 * config from Kafka.
 */
@UtilityClass
@Slf4j
@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
public final class TopicUtils {

    public static Optional<TopicDescription> describe(final String topic, final AdminClient admin)
            throws ExecutionException, InterruptedException {
        try {
            log.debug("Try to describe the {}", topic);
            final var topicsResult = admin.describeTopics(List.of(topic));
            final var descriptions = topicsResult.allTopicIds().get();
            log.debug("Description of {}: {}", topic, descriptions);
            return Optional.ofNullable(descriptions.get(topic));
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                return Optional.empty();
            } else {
                throw e;
            }
        }

    }

    public static Optional<Config> configsOf(final String topic, final AdminClient admin)
            throws ExecutionException, InterruptedException {
        log.debug("Try to get configs of {}", topic);
        final var resource = new ConfigResource(Type.TOPIC, topic);
        final var result = admin.describeConfigs(List.of(resource));

        final var config = result.all().get().get(resource);
        log.debug("Configs of {}: {}", topic, config);
        return Optional.ofNullable(config);
    }
}
