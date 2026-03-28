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
 * A resource to add or remove in an {@link Admin#alterVirtualCluster} call.
 */
public class AlterVirtualClusterResource {

    /** Resource type values. */
    public static final byte RESOURCE_TYPE_USER           = 0;
    public static final byte RESOURCE_TYPE_CLIENT         = 1;
    public static final byte RESOURCE_TYPE_TOPIC          = 2;
    public static final byte RESOURCE_TYPE_GROUP          = 3;
    public static final byte RESOURCE_TYPE_TRANSACTIONAL_ID = 4;

    /** Operation values. */
    public static final byte OPERATION_ADD    = 0;
    public static final byte OPERATION_REMOVE = 1;

    private final byte resourceType;
    private final String resourceName;
    private final String linkName;
    private final byte operation;

    /**
     * Create a resource entry for types that do not need a link name (user, client, group,
     * transactional-id).
     */
    public AlterVirtualClusterResource(byte resourceType, String resourceName, byte operation) {
        this(resourceType, resourceName, null, operation);
    }

    /**
     * Create a resource entry. {@code linkName} is required when {@code resourceType} is
     * {@link #RESOURCE_TYPE_TOPIC} and {@code operation} is {@link #OPERATION_ADD}.
     */
    public AlterVirtualClusterResource(byte resourceType, String resourceName, String linkName, byte operation) {
        this.resourceType = resourceType;
        this.resourceName = resourceName;
        this.linkName = linkName;
        this.operation = operation;
    }

    public byte resourceType() {
        return resourceType;
    }

    public String resourceName() {
        return resourceName;
    }

    /** May be {@code null} when not applicable. */
    public String linkName() {
        return linkName;
    }

    public byte operation() {
        return operation;
    }
}
