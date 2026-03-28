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

package org.apache.kafka.image;

import org.apache.kafka.image.node.VirtualClustersImageNode;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * Represents the virtual clusters in the metadata image.
 * <p>
 * This class is thread-safe.
 *
 * @param virtualClusterImages Map of virtual cluster name to its {@link VirtualClusterImage}.
 */
public record VirtualClustersImage(Map<String, VirtualClusterImage> virtualClusterImages) {
    public static final VirtualClustersImage EMPTY = new VirtualClustersImage(Map.of());

    public VirtualClustersImage(Map<String, VirtualClusterImage> virtualClusterImages) {
        this.virtualClusterImages = Collections.unmodifiableMap(virtualClusterImages);
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        for (VirtualClusterImage vc : virtualClusterImages.values()) {
            vc.write(writer, options);
        }
    }

    public boolean isEmpty() {
        return virtualClusterImages.isEmpty();
    }

    /**
     * Find the virtual cluster that contains the given principal as a user.
     *
     * @param principal the username to look up
     * @return the first {@link VirtualClusterImage} whose user list contains the principal,
     *         or {@link Optional#empty()} if no virtual cluster owns this principal
     */
    public Optional<VirtualClusterImage> findVcForUser(String principal) {
        return virtualClusterImages.values().stream()
            .filter(vc -> vc.users().contains(principal))
            .findFirst();
    }

    @Override
    public String toString() {
        return new VirtualClustersImageNode(this).stringify();
    }
}
