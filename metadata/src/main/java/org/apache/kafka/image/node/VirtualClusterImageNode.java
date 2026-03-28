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

package org.apache.kafka.image.node;

import org.apache.kafka.image.VirtualClusterImage;

import java.util.Arrays;
import java.util.Collection;

public class VirtualClusterImageNode implements MetadataNode {
    /**
     * The virtual cluster image.
     */
    private final VirtualClusterImage image;

    public VirtualClusterImageNode(VirtualClusterImage image) {
        this.image = image;
    }

    @Override
    public Collection<String> childNames() {
        return Arrays.asList("name", "topics", "users", "clients", "groups", "transactionalIds");
    }

    @Override
    public MetadataNode child(String name) {
        switch (name) {
            case "name":
                return new MetadataLeafNode(image.name());
            case "topics":
                return new MetadataLeafNode(image.topics().toString());
            case "users":
                return new MetadataLeafNode(image.users().toString());
            case "clients":
                return new MetadataLeafNode(image.clients().toString());
            case "groups":
                return new MetadataLeafNode(image.groups().toString());
            case "transactionalIds":
                return new MetadataLeafNode(image.transactionalIds().toString());
            default:
                return null;
        }
    }
}
