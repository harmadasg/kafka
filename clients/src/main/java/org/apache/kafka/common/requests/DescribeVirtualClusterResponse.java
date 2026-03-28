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

import org.apache.kafka.common.message.DescribeVirtualClusterResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.Map;
import java.util.stream.Collectors;

public class DescribeVirtualClusterResponse extends AbstractResponse {

    private final DescribeVirtualClusterResponseData data;

    public DescribeVirtualClusterResponse(DescribeVirtualClusterResponseData data) {
        super(ApiKeys.DESCRIBE_VIRTUAL_CLUSTER);
        this.data = data;
    }

    public static DescribeVirtualClusterResponse parse(Readable readable, short version) {
        return new DescribeVirtualClusterResponse(new DescribeVirtualClusterResponseData(readable, version));
    }

    @Override
    public DescribeVirtualClusterResponseData data() {
        return data;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return data.virtualClusters().stream()
            .collect(Collectors.toMap(
                vc -> Errors.forCode(vc.errorCode()),
                vc -> 1,
                Integer::sum));
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }
}
