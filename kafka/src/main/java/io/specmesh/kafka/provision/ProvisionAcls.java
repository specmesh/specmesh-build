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

import io.specmesh.kafka.KafkaApiSpec;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AclBinding;

public final class ProvisionAcls {

    /** defensive */
    private ProvisionAcls() {}
    /**
     * Provision acls in the Kafka cluster
     *
     * @param validateMode for mode of operation
     * @param apiSpec the api spec.
     * @param adminClient th admin client for the cluster.
     * @return status of provisioning
     * @throws Provisioner.ProvisioningException on interrupt
     */
    public static Status.Acls provision(
            final boolean validateMode, final KafkaApiSpec apiSpec, final Admin adminClient) {
        final var aclStatus = Status.Acls.builder();
        try {
            final Set<AclBinding> allAcls = apiSpec.requiredAcls();
            aclStatus.aclsToCreate(allAcls);
            if (!validateMode) {
                adminClient
                        .createAcls(allAcls)
                        .all()
                        .get(Provisioner.REQUEST_TIMEOUT, TimeUnit.SECONDS);
                aclStatus.aclsCreated(allAcls);
            }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            aclStatus.exception(new Provisioner.ProvisioningException("Failed to create ACLs", e));
        }
        return aclStatus.build();
    }
}
