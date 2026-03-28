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

import org.apache.kafka.common.metadata.VirtualClusterRecord;
import org.apache.kafka.image.node.VirtualClusterImageNode;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Represents the image of a single virtual cluster.
 * <p>
 * This class is thread-safe.
 */
public final class VirtualClusterImage {

    /**
     * A topic link associating a logical link name with a physical topic name.
     */
    public record TopicLink(String topicName, String linkName) {
        public TopicLink {
            Objects.requireNonNull(topicName, "topicName must not be null");
            Objects.requireNonNull(linkName, "linkName must not be null");
        }
    }

    private final String name;
    private final List<TopicLink> topics;
    private final List<String> users;
    private final List<String> clients;
    private final List<String> groups;
    private final List<String> transactionalIds;

    public VirtualClusterImage(
        String name,
        List<TopicLink> topics,
        List<String> users,
        List<String> clients,
        List<String> groups,
        List<String> transactionalIds
    ) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.topics = Collections.unmodifiableList(Objects.requireNonNull(topics, "topics must not be null"));
        this.users = Collections.unmodifiableList(Objects.requireNonNull(users, "users must not be null"));
        this.clients = Collections.unmodifiableList(Objects.requireNonNull(clients, "clients must not be null"));
        this.groups = Collections.unmodifiableList(Objects.requireNonNull(groups, "groups must not be null"));
        this.transactionalIds = Collections.unmodifiableList(Objects.requireNonNull(transactionalIds, "transactionalIds must not be null"));
    }

    /**
     * Constructs a {@link VirtualClusterImage} from a {@link VirtualClusterRecord}.
     */
    public static VirtualClusterImage fromRecord(VirtualClusterRecord record) {
        List<TopicLink> topicLinks = record.topics().stream()
            .map(t -> new TopicLink(t.topicName(), t.linkName()))
            .collect(Collectors.toList());
        return new VirtualClusterImage(
            record.name(),
            topicLinks,
            List.copyOf(record.users()),
            List.copyOf(record.clients()),
            List.copyOf(record.groups()),
            List.copyOf(record.transactionalIds())
        );
    }

    public String name() {
        return name;
    }

    public List<TopicLink> topics() {
        return topics;
    }

    public List<String> users() {
        return users;
    }

    public List<String> clients() {
        return clients;
    }

    public List<String> groups() {
        return groups;
    }

    public List<String> transactionalIds() {
        return transactionalIds;
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        if (options.metadataVersion().areVirtualClustersSupported()) {
            VirtualClusterRecord record = toRecord();
            writer.write(0, record);
        } else {
            options.handleLoss("virtual cluster " + name);
        }
    }

    /**
     * Converts this image back to a {@link VirtualClusterRecord} for writing to the metadata log.
     */
    public VirtualClusterRecord toRecord() {
        List<VirtualClusterRecord.VirtualClusterTopicLink> recordTopics = topics.stream()
            .map(t -> new VirtualClusterRecord.VirtualClusterTopicLink()
                .setTopicName(t.topicName())
                .setLinkName(t.linkName()))
            .collect(Collectors.toList());
        return new VirtualClusterRecord()
            .setName(name)
            .setTopics(recordTopics)
            .setUsers(new java.util.ArrayList<>(users))
            .setClients(new java.util.ArrayList<>(clients))
            .setGroups(new java.util.ArrayList<>(groups))
            .setTransactionalIds(new java.util.ArrayList<>(transactionalIds));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof VirtualClusterImage)) return false;
        VirtualClusterImage that = (VirtualClusterImage) o;
        return Objects.equals(name, that.name)
            && Objects.equals(topics, that.topics)
            && Objects.equals(users, that.users)
            && Objects.equals(clients, that.clients)
            && Objects.equals(groups, that.groups)
            && Objects.equals(transactionalIds, that.transactionalIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, topics, users, clients, groups, transactionalIds);
    }

    @Override
    public String toString() {
        return new VirtualClusterImageNode(this).stringify();
    }
}
