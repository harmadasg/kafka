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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.CreateVirtualClusterRequestData;
import org.apache.kafka.common.message.CreateVirtualClusterResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.ArrayList;
import java.util.List;

public class CreateVirtualClusterRequest extends AbstractRequest {

    private final CreateVirtualClusterRequestData data;

    private CreateVirtualClusterRequest(CreateVirtualClusterRequestData data, short version) {
        super(ApiKeys.CREATE_VIRTUAL_CLUSTER, version);
        this.data = data;
    }

    public static CreateVirtualClusterRequest parse(Readable readable, short version) {
        return new CreateVirtualClusterRequest(new CreateVirtualClusterRequestData(readable, version), version);
    }

    @Override
    public CreateVirtualClusterRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        List<CreateVirtualClusterResponseData.CreatableVirtualClusterResult> results = new ArrayList<>();
        for (CreateVirtualClusterRequestData.CreatableVirtualCluster vc : data.virtualClusters()) {
            results.add(new CreateVirtualClusterResponseData.CreatableVirtualClusterResult()
                .setName(vc.name())
                .setErrorCode(error.code())
                .setErrorMessage(e.getMessage()));
        }
        return new CreateVirtualClusterResponse(
            new CreateVirtualClusterResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setVirtualClusters(results));
    }

    public static class Builder extends AbstractRequest.Builder<CreateVirtualClusterRequest> {
        private final CreateVirtualClusterRequestData data;

        public Builder(CreateVirtualClusterRequestData data) {
            super(ApiKeys.CREATE_VIRTUAL_CLUSTER);
            this.data = data;
        }

        @Override
        public CreateVirtualClusterRequest build(short version) {
            return new CreateVirtualClusterRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }
}
