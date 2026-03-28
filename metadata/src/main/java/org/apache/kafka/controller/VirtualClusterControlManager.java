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

package org.apache.kafka.controller;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.AlterVirtualClusterRequestData;
import org.apache.kafka.common.message.AlterVirtualClusterRequestData.AlterableVirtualCluster;
import org.apache.kafka.common.metadata.RemoveVirtualClusterRecord;
import org.apache.kafka.common.metadata.VirtualClusterChangeRecord;
import org.apache.kafka.common.metadata.VirtualClusterRecord;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * The VirtualClusterControlManager manages virtual clusters stored in the __cluster_metadata topic.
 */
public class VirtualClusterControlManager {

    static class Builder {
        private LogContext logContext = null;
        private SnapshotRegistry snapshotRegistry = null;
        // nullable — if null, topic-existence check is skipped
        private ReplicationControlManager replicationControl = null;

        Builder setLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        Builder setSnapshotRegistry(SnapshotRegistry snapshotRegistry) {
            this.snapshotRegistry = snapshotRegistry;
            return this;
        }

        Builder setReplicationControl(ReplicationControlManager replicationControl) {
            this.replicationControl = replicationControl;
            return this;
        }

        VirtualClusterControlManager build() {
            if (logContext == null) logContext = new LogContext();
            if (snapshotRegistry == null) snapshotRegistry = new SnapshotRegistry(logContext);
            return new VirtualClusterControlManager(logContext, snapshotRegistry, replicationControl);
        }
    }

    private final Logger log;
    private final TimelineHashMap<String, VirtualClusterRecord> virtualClusters;
    // nullable — if null, topic-existence check is skipped
    private final ReplicationControlManager replicationControl;

    private VirtualClusterControlManager(
        LogContext logContext,
        SnapshotRegistry snapshotRegistry,
        ReplicationControlManager replicationControl
    ) {
        this.log = logContext.logger(VirtualClusterControlManager.class);
        this.virtualClusters = new TimelineHashMap<>(snapshotRegistry, 0);
        this.replicationControl = replicationControl;
    }

    ControllerResult<Void> createVirtualCluster(String name, MetadataVersion metadataVersion) {
        if (!metadataVersion.areVirtualClustersSupported()) {
            throw new UnsupportedVersionException(
                "Virtual clusters are not supported in metadata.version " + metadataVersion);
        }
        if (virtualClusters.containsKey(name)) {
            throw new InvalidRequestException("Virtual cluster '" + name + "' already exists.");
        }
        VirtualClusterRecord record = new VirtualClusterRecord()
            .setName(name)
            .setTopics(Collections.emptyList())
            .setUsers(Collections.emptyList())
            .setClients(Collections.emptyList())
            .setGroups(Collections.emptyList())
            .setTransactionalIds(Collections.emptyList());
        log.info("Creating virtual cluster '{}'.", name);
        return ControllerResult.of(
            List.of(new ApiMessageAndVersion(record, (short) 0)),
            null
        );
    }

    ControllerResult<Void> deleteVirtualCluster(String name, MetadataVersion metadataVersion) {
        if (!metadataVersion.areVirtualClustersSupported()) {
            throw new UnsupportedVersionException(
                "Virtual clusters are not supported in metadata.version " + metadataVersion);
        }
        if (!virtualClusters.containsKey(name)) {
            throw new InvalidRequestException("Virtual cluster '" + name + "' does not exist.");
        }
        VirtualClusterRecord existing = virtualClusters.get(name);
        List<String> problems = new ArrayList<>();
        if (!existing.topics().isEmpty())
            problems.add(existing.topics().size() + " topic(s)");
        if (!existing.users().isEmpty())
            problems.add(existing.users().size() + " user(s)");
        if (!existing.clients().isEmpty())
            problems.add(existing.clients().size() + " client(s)");
        if (!existing.groups().isEmpty())
            problems.add(existing.groups().size() + " group(s)");
        if (!existing.transactionalIds().isEmpty())
            problems.add(existing.transactionalIds().size() + " transactional-id(s)");
        if (!problems.isEmpty()) {
            throw new InvalidRequestException(
                "Virtual cluster '" + name + "' cannot be deleted because it is not empty: "
                + String.join(", ", problems) + ".");
        }
        RemoveVirtualClusterRecord record = new RemoveVirtualClusterRecord().setName(name);
        log.info("Deleting virtual cluster '{}'.", name);
        return ControllerResult.of(
            List.of(new ApiMessageAndVersion(record, (short) 0)),
            null
        );
    }

    ControllerResult<Void> alterVirtualCluster(AlterableVirtualCluster data, MetadataVersion metadataVersion) {
        if (!metadataVersion.areVirtualClustersSupported()) {
            throw new UnsupportedVersionException(
                "Virtual clusters are not supported in metadata.version " + metadataVersion);
        }
        String name = data.name();
        VirtualClusterRecord existing = virtualClusters.get(name);
        if (existing == null) {
            throw new InvalidRequestException("Virtual cluster '" + name + "' does not exist.");
        }

        // Working copies for validation only — we do not emit these as the new full state.
        List<VirtualClusterRecord.VirtualClusterTopicLink> topics = new ArrayList<>(existing.topics());
        List<String> users           = new ArrayList<>(existing.users());
        List<String> clients         = new ArrayList<>(existing.clients());
        List<String> groups          = new ArrayList<>(existing.groups());
        List<String> transactionalIds = new ArrayList<>(existing.transactionalIds());

        // Change-record accumulators — only the delta.
        List<VirtualClusterChangeRecord.VirtualClusterTopicLink> addedTopics = new ArrayList<>();
        List<String> removedTopics        = new ArrayList<>();
        List<String> addedUsers           = new ArrayList<>();
        List<String> removedUsers         = new ArrayList<>();
        List<String> addedClients         = new ArrayList<>();
        List<String> removedClients       = new ArrayList<>();
        List<String> addedGroups          = new ArrayList<>();
        List<String> removedGroups        = new ArrayList<>();
        List<String> addedTransactionalIds   = new ArrayList<>();
        List<String> removedTransactionalIds = new ArrayList<>();

        for (AlterVirtualClusterRequestData.AlterVirtualClusterResource resource : data.resources()) {
            byte op           = resource.operation();
            byte resourceType = resource.resourceType();
            String resourceName = resource.resourceName();

            switch (resourceType) {
                case 0: // USER
                    if (op == 0) checkNotAssignedElsewhere(name, "user", resourceName, VirtualClusterRecord::users);
                    applyStringOp(users, resourceName, op, "user", name);
                    if (op == 0) addedUsers.add(resourceName);
                    else removedUsers.add(resourceName);
                    break;
                case 1: // CLIENT
                    if (op == 0) checkNotAssignedElsewhere(name, "client", resourceName, VirtualClusterRecord::clients);
                    applyStringOp(clients, resourceName, op, "client", name);
                    if (op == 0) addedClients.add(resourceName);
                    else removedClients.add(resourceName);
                    break;
                case 2: // TOPIC
                    if (op == 0) { // ADD
                        String linkName = resource.linkName();
                        if (linkName == null || linkName.isEmpty()) {
                            throw new InvalidRequestException(
                                "LinkName is required when adding a topic to virtual cluster '" + name + "'.");
                        }
                        boolean linkNameInUse = topics.stream().anyMatch(t -> t.linkName().equals(linkName));
                        if (linkNameInUse) {
                            throw new InvalidRequestException(
                                "Link name '" + linkName + "' is already used in virtual cluster '" + name + "'.");
                        }
                        boolean topicExists = topics.stream().anyMatch(t -> t.topicName().equals(resourceName));
                        if (topicExists) {
                            throw new InvalidRequestException(
                                "Topic '" + resourceName + "' is already in virtual cluster '" + name + "'.");
                        }
                        if (replicationControl != null && replicationControl.getTopicId(resourceName) == null) {
                            throw new InvalidRequestException(
                                "Topic '" + resourceName + "' does not exist.");
                        }
                        topics.add(new VirtualClusterRecord.VirtualClusterTopicLink()
                            .setTopicName(resourceName)
                            .setLinkName(linkName));
                        addedTopics.add(new VirtualClusterChangeRecord.VirtualClusterTopicLink()
                            .setTopicName(resourceName)
                            .setLinkName(linkName));
                    } else { // REMOVE
                        boolean removed = topics.removeIf(t -> t.topicName().equals(resourceName));
                        if (!removed) {
                            throw new InvalidRequestException(
                                "Topic '" + resourceName + "' is not in virtual cluster '" + name + "'.");
                        }
                        removedTopics.add(resourceName);
                    }
                    break;
                case 3: // GROUP
                    if (op == 0) checkNotAssignedElsewhere(name, "group", resourceName, VirtualClusterRecord::groups);
                    applyStringOp(groups, resourceName, op, "group", name);
                    if (op == 0) addedGroups.add(resourceName);
                    else removedGroups.add(resourceName);
                    break;
                case 4: // TRANSACTIONAL_ID
                    if (op == 0) checkNotAssignedElsewhere(name, "transactional-id", resourceName, VirtualClusterRecord::transactionalIds);
                    applyStringOp(transactionalIds, resourceName, op, "transactional-id", name);
                    if (op == 0) addedTransactionalIds.add(resourceName);
                    else removedTransactionalIds.add(resourceName);
                    break;
                default:
                    throw new InvalidRequestException("Unknown resource type: " + resourceType);
            }
        }

        VirtualClusterChangeRecord changeRecord = new VirtualClusterChangeRecord()
            .setName(name)
            .setAddedTopics(addedTopics)
            .setRemovedTopics(removedTopics)
            .setAddedUsers(addedUsers)
            .setRemovedUsers(removedUsers)
            .setAddedClients(addedClients)
            .setRemovedClients(removedClients)
            .setAddedGroups(addedGroups)
            .setRemovedGroups(removedGroups)
            .setAddedTransactionalIds(addedTransactionalIds)
            .setRemovedTransactionalIds(removedTransactionalIds);

        log.info("Altering virtual cluster '{}'.", name);
        return ControllerResult.of(
            List.of(new ApiMessageAndVersion(changeRecord, (short) 0)),
            null
        );
    }

    private void checkNotAssignedElsewhere(String vcName, String typeName, String value,
                                             Function<VirtualClusterRecord, List<String>> getter) {
        for (Map.Entry<String, VirtualClusterRecord> entry : virtualClusters.entrySet()) {
            if (entry.getKey().equals(vcName)) continue;
            if (getter.apply(entry.getValue()).contains(value)) {
                throw new InvalidRequestException(
                    typeName + " '" + value + "' is already assigned to virtual cluster '" + entry.getKey() + "'.");
            }
        }
    }

    private static void applyStringOp(List<String> list, String value, byte op, String typeName, String vcName) {
        if (op == 0) { // ADD
            if (list.contains(value)) {
                throw new InvalidRequestException(
                    typeName + " '" + value + "' is already in virtual cluster '" + vcName + "'.");
            }
            list.add(value);
        } else { // REMOVE
            if (!list.remove(value)) {
                throw new InvalidRequestException(
                    typeName + " '" + value + "' is not in virtual cluster '" + vcName + "'.");
            }
        }
    }

    public void replay(VirtualClusterRecord record) {
        virtualClusters.put(record.name(), record);
        log.info("Replayed VirtualClusterRecord for '{}'.", record.name());
    }

    public void replay(RemoveVirtualClusterRecord record) {
        VirtualClusterRecord removed = virtualClusters.remove(record.name());
        if (removed == null) {
            throw new RuntimeException("Unable to replay " + record + ": no virtual cluster with name '" +
                record.name() + "' found.");
        }
        log.info("Replayed RemoveVirtualClusterRecord for '{}'.", record.name());
    }

    public void replay(VirtualClusterChangeRecord record) {
        VirtualClusterRecord existing = virtualClusters.get(record.name());
        if (existing == null) {
            throw new RuntimeException("Unable to replay " + record + ": no virtual cluster with name '" +
                record.name() + "' found.");
        }

        List<VirtualClusterRecord.VirtualClusterTopicLink> topics = new ArrayList<>(existing.topics());
        List<String> users            = new ArrayList<>(existing.users());
        List<String> clients          = new ArrayList<>(existing.clients());
        List<String> groups           = new ArrayList<>(existing.groups());
        List<String> transactionalIds = new ArrayList<>(existing.transactionalIds());

        for (VirtualClusterChangeRecord.VirtualClusterTopicLink added : record.addedTopics()) {
            topics.add(new VirtualClusterRecord.VirtualClusterTopicLink()
                .setTopicName(added.topicName())
                .setLinkName(added.linkName()));
        }
        for (String removedTopic : record.removedTopics()) {
            topics.removeIf(t -> t.topicName().equals(removedTopic));
        }
        users.addAll(record.addedUsers());
        users.removeAll(record.removedUsers());
        clients.addAll(record.addedClients());
        clients.removeAll(record.removedClients());
        groups.addAll(record.addedGroups());
        groups.removeAll(record.removedGroups());
        transactionalIds.addAll(record.addedTransactionalIds());
        transactionalIds.removeAll(record.removedTransactionalIds());

        VirtualClusterRecord updated = new VirtualClusterRecord()
            .setName(record.name())
            .setTopics(topics)
            .setUsers(users)
            .setClients(clients)
            .setGroups(groups)
            .setTransactionalIds(transactionalIds);
        virtualClusters.put(record.name(), updated);
        log.info("Replayed VirtualClusterChangeRecord for '{}'.", record.name());
    }

    List<String> listVirtualClusters(MetadataVersion metadataVersion) {
        if (!metadataVersion.areVirtualClustersSupported()) {
            throw new UnsupportedVersionException(
                "The current metadata version does not support virtual clusters.");
        }
        return new ArrayList<>(virtualClusters.keySet());
    }

    VirtualClusterRecord describeVirtualCluster(String name, MetadataVersion metadataVersion) {
        if (!metadataVersion.areVirtualClustersSupported()) {
            throw new UnsupportedVersionException(
                "The current metadata version does not support virtual clusters.");
        }
        VirtualClusterRecord record = virtualClusters.get(name);
        if (record == null) {
            throw new InvalidRequestException("Virtual cluster '" + name + "' does not exist.");
        }
        return record;
    }
}
