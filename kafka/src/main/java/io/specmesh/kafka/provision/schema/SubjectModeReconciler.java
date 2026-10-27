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

import static io.specmesh.kafka.provision.schema.SubjectMode.READONLY;
import static java.util.Objects.requireNonNull;

import io.specmesh.kafka.provision.Status.STATE;
import io.specmesh.kafka.provision.schema.SchemaProvisioner.Schema;
import io.specmesh.kafka.provision.schema.SubjectModes.SubjectModeException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

/**
 * Sets every key subject in the spec whose schema id is in the payload to {@code READONLY}.
 *
 * <p>This includes subjects that did not change in this run, so a subject made writable by hand is
 * made read-only again. Subjects in {@code READONLY_OVERRIDE} mode are already read-only and are
 * left alone.
 */
final class SubjectModeReconciler {

    private final boolean dryRun;
    private final SubjectModes modes;

    SubjectModeReconciler(final boolean dryRun, final SubjectModes modes) {
        this.dryRun = dryRun;
        this.modes = requireNonNull(modes, "modes");
    }

    /**
     * @param required the schemas required by the spec.
     * @param changeset the result of provisioning the changed schemas.
     * @return the changeset, plus any unchanged schemas whose subject mode was, or would be, set.
     */
    List<Schema> reconcile(final Collection<Schema> required, final List<Schema> changeset) {
        final Set<Schema> changed = Collections.newSetFromMap(new IdentityHashMap<>());
        changed.addAll(changeset);

        final List<Schema> result = new ArrayList<>(changeset);
        for (final Schema schema : required) {
            if (!schema.isKeyWithSchemaIdInPayload()) {
                continue;
            }

            if (changed.contains(schema)) {
                reconcileChanged(schema);
            } else if (reconcileUnchanged(schema)) {
                result.add(schema);
            }
        }
        return result;
    }

    private void reconcileChanged(final Schema schema) {
        if (schema.state() == STATE.CREATE && dryRun) {
            schema.addMessage("Subject mode would be set to: " + READONLY);
        } else if (schema.state() == STATE.CREATED || schema.state() == STATE.UPDATED) {
            makeReadOnly(schema);
        }
    }

    /**
     * @return true if the schema should be added to the output.
     */
    private boolean reconcileUnchanged(final Schema schema) {
        if (!makeReadOnly(schema)) {
            return false;
        }

        if (schema.state() != STATE.FAILED) {
            schema.state(dryRun ? STATE.UPDATE : STATE.UPDATED);
        }
        return true;
    }

    /**
     * @return false if the subject was already read-only, so nothing was attempted.
     */
    private boolean makeReadOnly(final Schema schema) {
        try {
            if (modes.get(schema.subject()).filter(SubjectMode::isReadOnly).isPresent()) {
                return false;
            }

            if (dryRun) {
                schema.addMessage("Subject mode would be set to: " + READONLY);
            } else {
                modes.set(schema.subject(), READONLY);
                schema.addMessage("Subject mode set to: " + READONLY);
            }
            return true;
        } catch (SubjectModeException e) {
            schema.exception(e);
            return true;
        }
    }
}
