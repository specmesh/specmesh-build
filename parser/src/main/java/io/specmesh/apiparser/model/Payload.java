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

package io.specmesh.apiparser.model;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.specmesh.apiparser.AsyncApiParser;
import io.specmesh.apiparser.util.JsonLocation;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Accessors(fluent = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Payload {

    private final PayloadPart key;
    private final PayloadPart value;

    public Payload(
            @JsonProperty(value = "key", required = true) final PayloadPart key,
            @JsonProperty(value = "value", required = true) final PayloadPart value) {
        this.key = requireNonNull(key, "key");
        this.value = requireNonNull(value, "value");
    }

    public PayloadPart key() {
        return key;
    }

    public PayloadPart value() {
        return value;
    }

    /**
     * @return The schemas in use
     */
    public Stream<TopicSchema> schemas(final String defaultSchemaLookupStrategy) {
        final Optional<TopicSchema> keySchema =
                key.schemaRef()
                        .map(
                                ref ->
                                        new TopicSchema(
                                                TopicSchema.Part.key,
                                                ref,
                                                defaultSchemaLookupStrategy));

        final Optional<TopicSchema> valSchema =
                value.schemaRef()
                        .map(
                                ref ->
                                        new TopicSchema(
                                                TopicSchema.Part.value,
                                                ref,
                                                defaultSchemaLookupStrategy));

        return Stream.of(keySchema, valSchema).flatMap(Optional::stream);
    }

    @JsonDeserialize(using = PayloadPartDeserializer.class)
    public interface PayloadPart {
        /**
         * @return reference to an external schema file.
         */
        default Optional<String> schemaRef() {
            return Optional.empty();
        }
    }

    @Value
    public static final class KafkaPart implements PayloadPart {
        // Types supported by for standard Kafka serializers:
        public enum KafkaType {
            UUID("uuid"),
            Long("long"),
            Int("int"),
            Short("short"),
            Float("float"),
            Double("double"),
            String("string"),
            Bytes("bytes"),
            Void("void");

            private static final String VALID_VALUES =
                    Arrays.stream(values())
                            .map(KafkaType::toString)
                            .collect(Collectors.joining(", ", "[", "]"));

            private final String text;

            KafkaType(final String text) {
                this.text = requireNonNull(text, "text");
            }

            @JsonValue
            @Override
            public String toString() {
                return text;
            }

            public static KafkaType fromText(final String text) {
                return Arrays.stream(values())
                        .filter(t -> t.text.equals(text))
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "Unknown KafkaType: "
                                                        + text
                                                        + ". Valid values are: "
                                                        + VALID_VALUES));
            }
        }

        private final KafkaType kafkaType;

        public KafkaPart(final KafkaType kafkaType) {
            this.kafkaType = requireNonNull(kafkaType, "kafkaType");
        }
    }

    @Value
    public static final class SchemaRefPart implements PayloadPart {
        private final String schemaRef;

        SchemaRefPart(final String schemaRef) {
            this.schemaRef = requireNonNull(schemaRef, "schemaRef");
        }

        public Optional<String> schemaRef() {
            return Optional.of(schemaRef);
        }
    }

    @Value
    public static final class OtherPart implements PayloadPart {
        private final JsonNode node;

        OtherPart(final JsonNode node) {
            this.node = requireNonNull(node, "node");
        }
    }

    private static class PayloadPartDeserializer extends JsonDeserializer<PayloadPart> {

        private static final String REF_FIELD = "$ref";
        private static final String TYPE_FIELD = "type";

        @Override
        public PayloadPart deserialize(final JsonParser parser, final DeserializationContext ctx)
                throws IOException {
            final URI location = JsonLocation.location(parser);

            if (parser.currentToken() != JsonToken.START_OBJECT) {
                throw new AsyncApiParser.APIParserException(
                        "Payload part should be an object with either "
                                + REF_FIELD
                                + " or "
                                + TYPE_FIELD
                                + ", but not both. location: "
                                + location);
            }

            parser.nextToken();

            Optional<String> ref = Optional.empty();
            Optional<String> type = Optional.empty();
            JsonNode props = ctx.getNodeFactory().objectNode();

            while (parser.currentToken() != JsonToken.END_OBJECT) {
                final String fieldName = parser.currentName();
                parser.nextToken();

                switch (fieldName) {
                    case REF_FIELD:
                        ref = Optional.of(ctx.readValue(parser, String.class));
                        break;
                    case TYPE_FIELD:
                        type = Optional.of(ctx.readValue(parser, String.class));
                        break;
                    case "properties":
                        props = ctx.readTree(parser);
                        break;
                    default:
                        ctx.readTree(parser);
                        break;
                }

                parser.nextToken();
            }

            if (ref.isPresent() && type.isPresent()) {
                throw new AsyncApiParser.APIParserException(
                        "Payload part requires either "
                                + REF_FIELD
                                + " or "
                                + TYPE_FIELD
                                + ", but not both. location: "
                                + location);
            }

            return ref.isPresent()
                    ? new SchemaRefPart(ref.get())
                    : type.filter(t -> !t.equals("object")).isPresent()
                            ? new KafkaPart(KafkaPart.KafkaType.fromText(type.get()))
                            : new OtherPart(props);
        }
    }
}
