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

import io.specmesh.kafka.provision.ProvisioningException;
import io.specmesh.kafka.provision.Status;
import io.specmesh.kafka.provision.schema.SchemaProvisioner.Part;
import io.specmesh.kafka.provision.schema.SchemaProvisioner.Schema;
import java.util.Optional;

/**
 * Fails calculated schema changes that are not safe to apply.
 *
 * <p>Kafka uses the serialized key to choose a partition. If the schema id is stored in the key
 * payload, any change to the key schema, even to its documentation, changes the schema id and so
 * the partition that records are routed to. Such changes are marked as failed.
 */
final class SchemaChangePolicy {

    /**
     * Enforce the policy on a calculated change.
     *
     * @param schema the schema, with the state set by the change set calculation.
     * @return the same schema, marked as failed if the change is not safe to apply.
     */
    Schema enforce(final Schema schema) {
        if (schema.state() != Status.STATE.UPDATE || !isKeyWithSchemaIdInPayload(schema)) {
            return schema;
        }

        return schema.exception(
                new ProvisioningException(
                        "Key schema changed for subject: "
                                + schema.subject()
                                + ", but schemaIdLocation is 'payload'. Changing the key schema"
                                + " changes the schema id embedded in the key bytes, which changes"
                                + " the partition records are routed to. Either store the schema id"
                                + " in a header or use a new topic."));
    }

    /**
     * @param schema the schema
     * @return true if the schema is a topic key schema with its schema id in the payload.
     */
    static boolean isKeyWithSchemaIdInPayload(final Schema schema) {
        return schema.topicSchema()
                && schema.part().equals(Optional.of(Part.KEY))
                && !schema.schemaIdLocation().orElse("payload").equalsIgnoreCase("header");
    }
}
