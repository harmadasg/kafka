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

import org.apache.kafka.common.message.AlterVirtualClusterRequestData;
import org.apache.kafka.common.message.AlterVirtualClusterResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.ArrayList;
import java.util.List;

public class AlterVirtualClusterRequest extends AbstractRequest {

    private final AlterVirtualClusterRequestData data;

    private AlterVirtualClusterRequest(AlterVirtualClusterRequestData data, short version) {
        super(ApiKeys.ALTER_VIRTUAL_CLUSTER, version);
        this.data = data;
    }

    public static AlterVirtualClusterRequest parse(Readable readable, short version) {
        return new AlterVirtualClusterRequest(new AlterVirtualClusterRequestData(readable, version), version);
    }

    @Override
    public AlterVirtualClusterRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        List<AlterVirtualClusterResponseData.AlterableVirtualClusterResult> results = new ArrayList<>();
        for (AlterVirtualClusterRequestData.AlterableVirtualCluster vc : data.virtualClusters()) {
            results.add(new AlterVirtualClusterResponseData.AlterableVirtualClusterResult()
                .setName(vc.name())
                .setErrorCode(error.code())
                .setErrorMessage(e.getMessage()));
        }
        return new AlterVirtualClusterResponse(
            new AlterVirtualClusterResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setVirtualClusters(results));
    }

    public static class Builder extends AbstractRequest.Builder<AlterVirtualClusterRequest> {
        private final AlterVirtualClusterRequestData data;

        public Builder(AlterVirtualClusterRequestData data) {
            super(ApiKeys.ALTER_VIRTUAL_CLUSTER);
            this.data = data;
        }

        @Override
        public AlterVirtualClusterRequest build(short version) {
            return new AlterVirtualClusterRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }
}
