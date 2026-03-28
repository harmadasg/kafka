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

import org.apache.kafka.common.message.DescribeVirtualClusterRequestData;
import org.apache.kafka.common.message.DescribeVirtualClusterResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.ArrayList;
import java.util.List;

public class DescribeVirtualClusterRequest extends AbstractRequest {

    private final DescribeVirtualClusterRequestData data;

    private DescribeVirtualClusterRequest(DescribeVirtualClusterRequestData data, short version) {
        super(ApiKeys.DESCRIBE_VIRTUAL_CLUSTER, version);
        this.data = data;
    }

    public static DescribeVirtualClusterRequest parse(Readable readable, short version) {
        return new DescribeVirtualClusterRequest(new DescribeVirtualClusterRequestData(readable, version), version);
    }

    @Override
    public DescribeVirtualClusterRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        List<DescribeVirtualClusterResponseData.DescribedVirtualCluster> results = new ArrayList<>();
        for (DescribeVirtualClusterRequestData.DescribableVirtualCluster vc : data.virtualClusters()) {
            results.add(new DescribeVirtualClusterResponseData.DescribedVirtualCluster()
                .setName(vc.name())
                .setErrorCode(error.code())
                .setErrorMessage(e.getMessage()));
        }
        return new DescribeVirtualClusterResponse(
            new DescribeVirtualClusterResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setVirtualClusters(results));
    }

    public static class Builder extends AbstractRequest.Builder<DescribeVirtualClusterRequest> {
        private final DescribeVirtualClusterRequestData data;

        public Builder(DescribeVirtualClusterRequestData data) {
            super(ApiKeys.DESCRIBE_VIRTUAL_CLUSTER);
            this.data = data;
        }

        @Override
        public DescribeVirtualClusterRequest build(short version) {
            return new DescribeVirtualClusterRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }
}
