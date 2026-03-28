/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.clients.admin;

/**
 * Options for {@link Admin#createVirtualClusters(java.util.Collection, CreateVirtualClusterOptions)}.
 */
public class CreateVirtualClusterOptions extends AbstractOptions<CreateVirtualClusterOptions> {

    private boolean validateOnly = false;
    private boolean retryOnQuotaViolation = true;

    /**
     * Set the timeout in milliseconds for this operation, or {@code null} if the default
     * API timeout for the AdminClient should be used.
     */
    public CreateVirtualClusterOptions timeoutMs(Integer timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
    }

    /**
     * If {@code true}, only validate the request without actually creating the virtual cluster(s).
     */
    public CreateVirtualClusterOptions validateOnly(boolean validateOnly) {
        this.validateOnly = validateOnly;
        return this;
    }

    public boolean validateOnly() {
        return validateOnly;
    }

    /**
     * If {@code true}, client will retry when quota is violated.
     */
    public CreateVirtualClusterOptions retryOnQuotaViolation(boolean retryOnQuotaViolation) {
        this.retryOnQuotaViolation = retryOnQuotaViolation;
        return this;
    }

    public boolean retryOnQuotaViolation() {
        return retryOnQuotaViolation;
    }
}
