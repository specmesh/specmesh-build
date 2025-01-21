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

package io.specmesh.kafka.provision.schema;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;

final class SchemaOwnership {

    private SchemaOwnership() {}

    /**
     * Determine if a schema is owned by a domain.
     *
     * <p>Currently only Avro schemas ownership can be determined.
     *
     * @param schema the schema to check
     * @param domainId the potential owning domain.
     * @return {@code true} if the schema is owned by the supplied {@code domainId}, or if ownership
     *     could not be determined.
     */
    static boolean schemaOwnedByDomain(
            final SchemaProvisioner.Schema schema, final String domainId) {
        if (schema.schema() instanceof AvroSchema) {
            final AvroSchema avroSchema = (AvroSchema) schema.schema();
            return avroSchema.rawSchema().getNamespace().equals(domainId);
        }

        return true;
    }
}
