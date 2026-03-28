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

import java.util.Collections;
import java.util.List;

/**
 * A set of changes to apply to a single virtual cluster in an
 * {@link Admin#alterVirtualClusters(java.util.Collection, AlterVirtualClusterOptions)} call.
 *
 * <p>The public API uses typed enums ({@link ResourceType}, {@link ResourceChangeType}) instead of
 * raw byte constants. Internally, {@link KafkaAdminClient} maps these to the wire-level byte
 * constants defined in {@link AlterVirtualClusterResource}.
 */
public class VirtualClusterAlteration {

    /**
     * The type of resource being added to or removed from a virtual cluster.
     *
     * <p>Note: The KIP-1134 spec lists USER, TOPIC, GROUP, TRANSACTIONAL_ID. This implementation
     * also includes CLIENT, which the KIP omitted but which is required for client-id isolation.
     */
    public enum ResourceType {
        USER,
        CLIENT,
        TOPIC,
        GROUP,
        TRANSACTIONAL_ID
    }

    /**
     * Whether the resource is being added to or removed from the virtual cluster.
     */
    public enum ResourceChangeType {
        ADD,
        REMOVE
    }

    /**
     * A single entity change: one resource being added to or removed from a virtual cluster.
     */
    public static class VirtualClusterEntityChange {

        private final ResourceType resourceType;
        private final String resourceName;
        /** Non-null only when {@code resourceType == TOPIC} and {@code changeType == ADD}. */
        private final String linkName;
        private final ResourceChangeType changeType;

        /**
         * Constructor for non-topic resources (USER, CLIENT, GROUP, TRANSACTIONAL_ID).
         */
        public VirtualClusterEntityChange(ResourceType resourceType, String resourceName,
                                          ResourceChangeType changeType) {
            this(resourceType, resourceName, null, changeType);
        }

        /**
         * Constructor for all resource types. {@code linkName} is required when
         * {@code resourceType == TOPIC} and {@code changeType == ADD}.
         */
        public VirtualClusterEntityChange(ResourceType resourceType, String resourceName,
                                          String linkName, ResourceChangeType changeType) {
            this.resourceType = resourceType;
            this.resourceName = resourceName;
            this.linkName = linkName;
            this.changeType = changeType;
        }

        public ResourceType resourceType() {
            return resourceType;
        }

        public String resourceName() {
            return resourceName;
        }

        /** May be {@code null} when not applicable. */
        public String linkName() {
            return linkName;
        }

        public ResourceChangeType changeType() {
            return changeType;
        }

        @Override
        public String toString() {
            return "VirtualClusterEntityChange(" +
                "resourceType=" + resourceType +
                ", resourceName=" + resourceName +
                ", linkName=" + linkName +
                ", changeType=" + changeType + ")";
        }
    }

    private final String virtualClusterName;
    private final List<VirtualClusterEntityChange> changes;

    /**
     * Create an alteration spec for the given virtual cluster.
     *
     * @param virtualClusterName The name of the virtual cluster to alter.
     * @param changes            The list of entity changes to apply.
     */
    public VirtualClusterAlteration(String virtualClusterName,
                                    List<VirtualClusterEntityChange> changes) {
        this.virtualClusterName = virtualClusterName;
        this.changes = Collections.unmodifiableList(changes);
    }

    /**
     * The name of the virtual cluster to alter.
     */
    public String virtualClusterName() {
        return virtualClusterName;
    }

    /**
     * The list of entity changes to apply to this virtual cluster.
     */
    public List<VirtualClusterEntityChange> changes() {
        return changes;
    }

    @Override
    public String toString() {
        return "VirtualClusterAlteration(virtualClusterName=" + virtualClusterName +
            ", changes=" + changes + ")";
    }
}
