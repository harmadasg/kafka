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

import org.apache.kafka.common.metadata.VirtualClusterChangeRecord;
import org.apache.kafka.common.metadata.VirtualClusterRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Represents the changes to a single virtual cluster within one metadata batch.
 * <p>
 * Each resource type is tracked as two separate lists: one for additions and one for removals.
 * The diff is computed eagerly when {@link #replay(VirtualClusterRecord)} is called by comparing
 * the incoming record against the base image.
 */
public final class VirtualClusterDelta {

    /**
     * The base image before this batch of changes.
     */
    private final VirtualClusterImage image;

    private final List<VirtualClusterImage.TopicLink> addedTopicLinks = new ArrayList<>();
    private final List<VirtualClusterImage.TopicLink> removedTopicLinks = new ArrayList<>();
    private final List<String> addedUsers = new ArrayList<>();
    private final List<String> removedUsers = new ArrayList<>();
    private final List<String> addedClients = new ArrayList<>();
    private final List<String> removedClients = new ArrayList<>();
    private final List<String> addedGroups = new ArrayList<>();
    private final List<String> removedGroups = new ArrayList<>();
    private final List<String> addedTransactionalIds = new ArrayList<>();
    private final List<String> removedTransactionalIds = new ArrayList<>();

    /**
     * The latest full image after replaying the most recent record. May be updated multiple times
     * within the same delta if multiple records arrive for the same VC in one batch.
     */
    private VirtualClusterImage latestImage;

    public VirtualClusterDelta(VirtualClusterImage image) {
        this.image = Objects.requireNonNull(image, "image must not be null");
        this.latestImage = image;
    }

    /**
     * Replays an incoming {@link VirtualClusterRecord} against the current latest image,
     * computing the diff and updating all added/removed lists.
     */
    public void replay(VirtualClusterRecord record) {
        VirtualClusterImage newImage = VirtualClusterImage.fromRecord(record);

        // Diff topic links
        Set<VirtualClusterImage.TopicLink> oldTopics = new HashSet<>(latestImage.topics());
        Set<VirtualClusterImage.TopicLink> newTopics = new HashSet<>(newImage.topics());
        for (VirtualClusterImage.TopicLink t : newTopics) {
            if (!oldTopics.contains(t)) addedTopicLinks.add(t);
        }
        for (VirtualClusterImage.TopicLink t : oldTopics) {
            if (!newTopics.contains(t)) removedTopicLinks.add(t);
        }

        // Diff string resource lists
        diffStrings(latestImage.users(), newImage.users(), addedUsers, removedUsers);
        diffStrings(latestImage.clients(), newImage.clients(), addedClients, removedClients);
        diffStrings(latestImage.groups(), newImage.groups(), addedGroups, removedGroups);
        diffStrings(latestImage.transactionalIds(), newImage.transactionalIds(), addedTransactionalIds, removedTransactionalIds);

        latestImage = newImage;
    }

    /**
     * Replays a {@link VirtualClusterChangeRecord} — a sparse delta record that carries only
     * what changed. Directly populates the added/removed lists without a full diff.
     */
    public void replay(VirtualClusterChangeRecord record) {
        // Apply topic link changes
        List<VirtualClusterImage.TopicLink> newTopics = new ArrayList<>(latestImage.topics());
        for (VirtualClusterChangeRecord.VirtualClusterTopicLink added : record.addedTopics()) {
            VirtualClusterImage.TopicLink link = new VirtualClusterImage.TopicLink(added.topicName(), added.linkName());
            addedTopicLinks.add(link);
            newTopics.add(link);
        }
        for (String removedTopic : record.removedTopics()) {
            newTopics.removeIf(t -> {
                if (t.topicName().equals(removedTopic)) {
                    removedTopicLinks.add(t);
                    return true;
                }
                return false;
            });
        }

        // Apply string resource changes
        List<String> newUsers = applyChanges(latestImage.users(), record.addedUsers(), record.removedUsers(), addedUsers, removedUsers);
        List<String> newClients = applyChanges(latestImage.clients(), record.addedClients(), record.removedClients(), addedClients, removedClients);
        List<String> newGroups = applyChanges(latestImage.groups(), record.addedGroups(), record.removedGroups(), addedGroups, removedGroups);
        List<String> newTransactionalIds = applyChanges(latestImage.transactionalIds(), record.addedTransactionalIds(), record.removedTransactionalIds(), addedTransactionalIds, removedTransactionalIds);

        latestImage = new VirtualClusterImage(
            latestImage.name(),
            newTopics,
            newUsers,
            newClients,
            newGroups,
            newTransactionalIds
        );
    }

    /**
     * Applies a set of additions and removals to a base list, also tracking cumulative changes.
     */
    private static List<String> applyChanges(
        List<String> base,
        List<String> added,
        List<String> removed,
        List<String> cumulativeAdded,
        List<String> cumulativeRemoved
    ) {
        List<String> result = new ArrayList<>(base);
        cumulativeAdded.addAll(added);
        cumulativeRemoved.addAll(removed);
        result.addAll(added);
        result.removeAll(removed);
        return result;
    }

    private static void diffStrings(
        List<String> oldList,
        List<String> newList,
        List<String> added,
        List<String> removed
    ) {
        Set<String> oldSet = new HashSet<>(oldList);
        Set<String> newSet = new HashSet<>(newList);
        for (String s : newSet) {
            if (!oldSet.contains(s)) added.add(s);
        }
        for (String s : oldSet) {
            if (!newSet.contains(s)) removed.add(s);
        }
    }

    /**
     * Applies the accumulated changes to produce a new {@link VirtualClusterImage}.
     */
    public VirtualClusterImage apply() {
        return latestImage;
    }

    public VirtualClusterImage image() {
        return image;
    }

    public List<VirtualClusterImage.TopicLink> addedTopicLinks() {
        return Collections.unmodifiableList(addedTopicLinks);
    }

    public List<VirtualClusterImage.TopicLink> removedTopicLinks() {
        return Collections.unmodifiableList(removedTopicLinks);
    }

    public List<String> addedUsers() {
        return Collections.unmodifiableList(addedUsers);
    }

    public List<String> removedUsers() {
        return Collections.unmodifiableList(removedUsers);
    }

    public List<String> addedClients() {
        return Collections.unmodifiableList(addedClients);
    }

    public List<String> removedClients() {
        return Collections.unmodifiableList(removedClients);
    }

    public List<String> addedGroups() {
        return Collections.unmodifiableList(addedGroups);
    }

    public List<String> removedGroups() {
        return Collections.unmodifiableList(removedGroups);
    }

    public List<String> addedTransactionalIds() {
        return Collections.unmodifiableList(addedTransactionalIds);
    }

    public List<String> removedTransactionalIds() {
        return Collections.unmodifiableList(removedTransactionalIds);
    }

    @Override
    public String toString() {
        return "VirtualClusterDelta(" +
            "name=" + image.name() +
            ", addedTopicLinks=" + addedTopicLinks +
            ", removedTopicLinks=" + removedTopicLinks +
            ", addedUsers=" + addedUsers +
            ", removedUsers=" + removedUsers +
            ", addedClients=" + addedClients +
            ", removedClients=" + removedClients +
            ", addedGroups=" + addedGroups +
            ", removedGroups=" + removedGroups +
            ", addedTransactionalIds=" + addedTransactionalIds +
            ", removedTransactionalIds=" + removedTransactionalIds +
            ")";
    }
}
