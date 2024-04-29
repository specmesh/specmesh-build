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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.specmesh.apiparser.AsyncApiParser;
import java.util.Map;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Accessors(fluent = true)
@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
@SuppressFBWarnings
public class Payload {

    @JsonProperty("$ref")
    private String ref;

    @JsonProperty("type")
    private String typeT;

    @JsonProperty private PayloadProperties properties;

    public PayloadType payload() {
        if (this.ref != null) {
            return new SchemaReference(new SchemaRefType(ref));
        }
        if (properties.isKeyValue()) {
            return new KeyValue(properties.key(), properties.value());
        }
        if (properties.isObject()) {
            return new Object(properties.otherProperties());
        }
        throw new AsyncApiParser.APIParserException("Cannot get Type from:");
    }

    public static class KeyValue implements PayloadType {
        private final TypeInfo key;
        private final TypeInfo value;

        public KeyValue(
                final PayloadProperties.KeyValue key, final PayloadProperties.KeyValue value) {
            this.key = asType(key);
            this.value = asType(value);
        }

        public TypeInfo key() {
            return key;
        }

        public TypeInfo value() {
            return value;
        }

        private TypeInfo asType(final PayloadProperties.KeyValue keyValue) {
            if (keyValue.type() != null) {
                return PrimitiveType.fromTypeName(keyValue.type());
            }
            if (keyValue.ref() != null) {
                return new SchemaRefType(keyValue.ref());
            }
            throw new AsyncApiParser.APIParserException("Cannot get Type");
        }
    }

    public static class Object implements PayloadType {
        private final Map<String, java.lang.Object> properties;

        public Object(final Map<String, java.lang.Object> properties) {
            this.properties = properties;
        }

        public Map<String, java.lang.Object> properties() {
            return properties;
        }
    }

    public static class SchemaReference implements PayloadType {
        private final SchemaRefType reference;

        public SchemaReference(final SchemaRefType schemaRefType) {
            this.reference = schemaRefType;
        }

        public SchemaRefType reference() {
            return reference;
        }
    }

    public interface PayloadType {}

    /** Types */
    public interface TypeInfo {
        String value();
    }
    /** $ref: "MessageValue.avsc" */
    public static class SchemaRefType implements TypeInfo {
        private final String reference;

        public SchemaRefType(final String schema) {
            this.reference = schema;
        }

        public String schema() {
            return reference;
        }

        @Override
        public String value() {
            return reference;
        }
    }

    /**
     * this: value: type: double These: Set.of("UUID", "long", "int", "short", "float", "double",
     * "string", "bytes", "$ref");
     */
    public enum PrimitiveType implements TypeInfo {
        STRING(new TypeDefinition<>(String.class, "string")),
        INT(new TypeDefinition<>(Integer.class, "int")),
        DOUBLE(new TypeDefinition<>(Double.class, "double"));
        private final TypeDefinition<?> typeDefinition;

        PrimitiveType(final TypeDefinition<?> typeDefinition) {
            this.typeDefinition = typeDefinition;
        }

        @SuppressWarnings("unchecked")
        public <T> TypeDefinition<T> getTypeDefinition() {
            if (typeDefinition instanceof TypeDefinition<?>) {
                return (TypeDefinition<T>) typeDefinition;
            }
            throw new AsyncApiParser.APIParserException("Failedto get 'TypeDefinition");
        }

        public static <T> PrimitiveType fromTypeName(final String typeName) {
            for (PrimitiveType type : values()) {
                if (type.getTypeDefinition().getTypeName().equalsIgnoreCase(typeName)) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Invalid type name: " + typeName);
        }

        @Override
        public String value() {
            return this.typeDefinition.typeName;
        }

        private static class TypeDefinition<T> {
            private final Class<T> clazz;
            private final String typeName;

            TypeDefinition(final Class<T> clazz, final String typeName) {
                this.clazz = clazz;
                this.typeName = typeName;
            }

            public Class<T> getClazz() {
                return clazz;
            }

            public String getTypeName() {
                return typeName;
            }
        }
    }
}
