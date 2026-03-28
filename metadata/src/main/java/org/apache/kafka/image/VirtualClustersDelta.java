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

import org.apache.kafka.common.metadata.RemoveVirtualClusterRecord;
import org.apache.kafka.common.metadata.VirtualClusterChangeRecord;
import org.apache.kafka.common.metadata.VirtualClusterRecord;
import org.apache.kafka.server.common.MetadataVersion;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Represents changes to the virtual clusters in the metadata image.
 * <p>
 * Each entry in {@link #changes} maps a virtual cluster name to either:
 * <ul>
 *   <li>an empty {@link Optional} — the virtual cluster was deleted, or</li>
 *   <li>a non-empty {@link Optional} containing a {@link VirtualClusterDelta} — the VC was
 *       created or modified, with per-resource add/remove lists populated by diffing.</li>
 * </ul>
 */
public final class VirtualClustersDelta {
    private final VirtualClustersImage image;
    private final Map<String, Optional<VirtualClusterDelta>> changes = new HashMap<>();

    public VirtualClustersDelta(VirtualClustersImage image) {
        this.image = image;
    }

    public VirtualClustersImage image() {
        return image;
    }

    /**
     * Returns the map of per-cluster deltas. An empty Optional indicates deletion; a present
     * Optional carries the per-resource diff.
     */
    public Map<String, Optional<VirtualClusterDelta>> changes() {
        return Collections.unmodifiableMap(changes);
    }

    public void replay(VirtualClusterRecord record) {
        String name = record.name();
        Optional<VirtualClusterDelta> existing = changes.get(name);
        VirtualClusterDelta delta;
        if (existing != null && existing.isPresent()) {
            // Already have a delta for this VC in this batch — update it
            delta = existing.get();
        } else {
            // First record for this VC in this batch — base image is either the existing image or empty
            VirtualClusterImage base = image.virtualClusterImages().getOrDefault(
                name,
                new VirtualClusterImage(name, List.of(), List.of(), List.of(), List.of(), List.of())
            );
            delta = new VirtualClusterDelta(base);
        }
        delta.replay(record);
        changes.put(name, Optional.of(delta));
    }

    public void replay(VirtualClusterChangeRecord record) {
        String name = record.name();
        Optional<VirtualClusterDelta> existing = changes.get(name);
        VirtualClusterDelta delta;
        if (existing != null && existing.isPresent()) {
            delta = existing.get();
        } else {
            VirtualClusterImage base = image.virtualClusterImages().getOrDefault(
                name,
                new VirtualClusterImage(name, List.of(), List.of(), List.of(), List.of(), List.of())
            );
            delta = new VirtualClusterDelta(base);
        }
        delta.replay(record);
        changes.put(name, Optional.of(delta));
    }

    /**
     * Replays a {@link RemoveVirtualClusterRecord}. If the VC existed in the base image it is
     * marked as deleted. If it was created within the same batch the addition is cancelled out.
     */
    public void replay(RemoveVirtualClusterRecord record) {
        String name = record.name();
        if (image.virtualClusterImages().containsKey(name)) {
            // Existed in base image — mark as deleted
            changes.put(name, Optional.empty());
        } else if (changes.containsKey(name)) {
            // Was added within this same delta batch — cancel out the addition
            changes.remove(name);
        } else {
            throw new IllegalStateException(
                "Tried to remove virtual cluster " + name + " which does not exist.");
        }
    }

    public void handleMetadataVersionChange(MetadataVersion newVersion) {
        // no-op
    }

    public void finishSnapshot() {
        for (String name : image.virtualClusterImages().keySet()) {
            if (!changes.containsKey(name)) {
                changes.put(name, Optional.empty());
            }
        }
    }

    public VirtualClustersImage apply() {
        Map<String, VirtualClusterImage> newImages = new HashMap<>();
        // Start from base image, applying any changes
        for (Map.Entry<String, VirtualClusterImage> entry : image.virtualClusterImages().entrySet()) {
            Optional<VirtualClusterDelta> change = changes.get(entry.getKey());
            if (change == null) {
                // No change — carry forward as-is
                newImages.put(entry.getKey(), entry.getValue());
            } else if (change.isPresent()) {
                // Updated
                newImages.put(entry.getKey(), change.get().apply());
            }
            // else: deleted — omit
        }
        // Add newly created VCs (those not in base image)
        for (Map.Entry<String, Optional<VirtualClusterDelta>> entry : changes.entrySet()) {
            if (!image.virtualClusterImages().containsKey(entry.getKey()) && entry.getValue().isPresent()) {
                newImages.put(entry.getKey(), entry.getValue().get().apply());
            }
        }
        return new VirtualClustersImage(newImages);
    }

    @Override
    public String toString() {
        return "VirtualClustersDelta(" +
            "changes=" + changes.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(", ")) + ")";
    }
}
