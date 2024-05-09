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
import lombok.Value;

/**
 * Metadata describing a part of a Kafka record.
 *
 * <p>e.g. the key or value of a record.
 */
@JsonDeserialize(using = RecordPart.Deserializer.class)
public interface RecordPart {
    /**
     * @return reference to an external schema file.
     */
    default Optional<String> schemaRef() {
        return Optional.empty();
    }

    default Optional<KafkaType> kafkaType() {
        return Optional.empty();
    }

    // Types supported by for standard Kafka serializers:
    enum KafkaType {
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
                        .map(RecordPart.KafkaType::toString)
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

        public static KafkaPart.KafkaType fromText(final String text) {
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

    @Value
    final class KafkaPart implements RecordPart {

        private final KafkaPart.KafkaType kafkaType;

        public KafkaPart(final KafkaPart.KafkaType kafkaType) {
            this.kafkaType = requireNonNull(kafkaType, "kafkaType");
        }

        @Override
        public Optional<KafkaType> kafkaType() {
            return Optional.of(kafkaType);
        }
    }

    @Value
    final class SchemaRefPart implements RecordPart {

        private final String schemaRef;

        public SchemaRefPart(final String schemaRef) {
            this.schemaRef = requireNonNull(schemaRef, "schemaRef");
        }

        public Optional<String> schemaRef() {
            return Optional.of(schemaRef);
        }
    }

    @Value
    final class OtherPart implements RecordPart {

        private final JsonNode node;

        public OtherPart(final JsonNode node) {
            this.node = requireNonNull(node, "node");
        }
    }

    final class Deserializer extends JsonDeserializer<RecordPart> {

        private static final String REF_FIELD = "$ref";
        private static final String TYPE_FIELD = "type";

        @Override
        public RecordPart deserialize(final JsonParser parser, final DeserializationContext ctx)
                throws IOException {
            final URI location = JsonLocation.location(parser);

            if (parser.currentToken() != JsonToken.START_OBJECT) {
                throw new AsyncApiParser.APIParserException(
                        "Record part should be an object with either "
                                + REF_FIELD
                                + " or "
                                + TYPE_FIELD
                                + ". location: "
                                + location);
            }

            parser.nextToken();

            Optional<String> ref = Optional.empty();
            Optional<String> type = Optional.empty();
            Optional<JsonNode> props = Optional.empty();

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
                        props = Optional.of(ctx.readTree(parser));
                        break;
                    default:
                        ctx.readTree(parser);
                        break;
                }

                parser.nextToken();
            }

            if (ref.isPresent() == type.isPresent()) {
                throw new AsyncApiParser.APIParserException(
                        "Record part requires either "
                                + REF_FIELD
                                + " or "
                                + TYPE_FIELD
                                + ", but not both. location: "
                                + location);
            }

            if (ref.isPresent()) {
                return new SchemaRefPart(ref.get());
            }

            if (!"object".equals(type.get())) {
                return new KafkaPart(KafkaType.fromText(type.get()));
            }

            return new OtherPart(
                    props.orElseThrow(
                            () ->
                                    new AsyncApiParser.APIParserException(
                                            "'object' types requires inline 'properties. location: "
                                                    + location)));
        }
    }
}
