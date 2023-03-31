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

package io.specmesh.kafka.provision;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collection;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/** Accumulated Provision status of Underlying resources */
@Builder
@Data
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Accessors(fluent = true)
@SuppressFBWarnings
public class Status {

    private Collection<TopicProvisioner.Topic> topics;
    private Collection<SchemaProvisioner.Schema> schemas;
    private Collection<AclProvisioner.Acl> acls;

    /** Operation result */
    public enum STATE {
        /** represents READ state */
        READ,
        /** intention to create */
        CREATE,
        /** successfully creates */
        CREATED,
        /** intention to update */
        UPDATE,
        /** updated successfully */
        UPDATED,
        /** intention to delete */
        DELETE,
        /** deleted successfully */
        DELETED,
        /** Noop */
        IGNORED,
        /** operation failed */
        FAILED
    }
}
