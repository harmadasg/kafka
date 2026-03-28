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
import org.apache.kafka.common.message.AlterVirtualClusterRequestData.AlterVirtualClusterResource;
import org.apache.kafka.common.message.AlterVirtualClusterRequestData.AlterableVirtualCluster;
import org.apache.kafka.common.metadata.RemoveVirtualClusterRecord;
import org.apache.kafka.common.metadata.VirtualClusterChangeRecord;
import org.apache.kafka.common.metadata.VirtualClusterRecord;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Timeout(value = 40)
public class VirtualClusterControlManagerTest {

    private static VirtualClusterControlManager newManager() {
        return new VirtualClusterControlManager.Builder()
            .setLogContext(new LogContext())
            .setSnapshotRegistry(new SnapshotRegistry(new LogContext()))
            .build();
    }

    /**
     * Builds a manager backed by a mock ReplicationControlManager that treats
     * the given topic names as existing (getTopicId returns non-null) and any
     * other name as absent (getTopicId returns null).
     */
    private static VirtualClusterControlManager newManagerWithTopics(String... existingTopics) {
        ReplicationControlManager rcm = mock(ReplicationControlManager.class);
        // by default getTopicId returns null (topic does not exist)
        when(rcm.getTopicId(org.mockito.ArgumentMatchers.anyString())).thenReturn(null);
        for (String topic : existingTopics) {
            when(rcm.getTopicId(topic)).thenReturn(org.apache.kafka.common.Uuid.randomUuid());
        }
        return new VirtualClusterControlManager.Builder()
            .setLogContext(new LogContext())
            .setSnapshotRegistry(new SnapshotRegistry(new LogContext()))
            .setReplicationControl(rcm)
            .build();
    }

    @Test
    public void testCreateVirtualCluster() {
        VirtualClusterControlManager manager = newManager();

        ControllerResult<Void> result = manager.createVirtualCluster("my_cluster", MetadataVersion.IBP_4_4_IV0);

        List<ApiMessageAndVersion> records = result.records();
        assertEquals(1, records.size());
        assertEquals(VirtualClusterRecord.class, records.get(0).message().getClass());
        assertEquals("my_cluster", ((VirtualClusterRecord) records.get(0).message()).name());

        RecordTestUtils.replayAll(manager, records);

        // After replay, creating the same name again must fail
        assertThrows(InvalidRequestException.class,
            () -> manager.createVirtualCluster("my_cluster", MetadataVersion.IBP_4_4_IV0));
    }

    @Test
    public void testCreateVirtualClusterUnsupportedVersion() {
        VirtualClusterControlManager manager = newManager();

        assertThrows(UnsupportedVersionException.class,
            () -> manager.createVirtualCluster("my_cluster", MetadataVersion.IBP_4_3_IV0));
    }

    @Test
    public void testCreateDuplicateVirtualCluster() {
        VirtualClusterControlManager manager = newManager();

        ControllerResult<Void> result = manager.createVirtualCluster("dup_cluster", MetadataVersion.IBP_4_4_IV0);
        RecordTestUtils.replayAll(manager, result.records());

        assertThrows(InvalidRequestException.class,
            () -> manager.createVirtualCluster("dup_cluster", MetadataVersion.IBP_4_4_IV0));
    }

    @Test
    public void testDeleteVirtualCluster() {
        VirtualClusterControlManager manager = newManager();

        // Create and replay first
        RecordTestUtils.replayAll(manager,
            manager.createVirtualCluster("to_delete", MetadataVersion.IBP_4_4_IV0).records());

        ControllerResult<Void> result = manager.deleteVirtualCluster("to_delete", MetadataVersion.IBP_4_4_IV0);

        List<ApiMessageAndVersion> records = result.records();
        assertEquals(1, records.size());
        assertEquals(RemoveVirtualClusterRecord.class, records.get(0).message().getClass());
        assertEquals("to_delete", ((RemoveVirtualClusterRecord) records.get(0).message()).name());

        RecordTestUtils.replayAll(manager, records);

        // After replay, the cluster is gone — re-create must succeed
        ControllerResult<Void> recreate = manager.createVirtualCluster("to_delete", MetadataVersion.IBP_4_4_IV0);
        assertEquals(1, recreate.records().size());
    }

    @Test
    public void testDeleteVirtualClusterUnsupportedVersion() {
        VirtualClusterControlManager manager = newManager();

        assertThrows(UnsupportedVersionException.class,
            () -> manager.deleteVirtualCluster("any_cluster", MetadataVersion.IBP_4_3_IV0));
    }

    @Test
    public void testDeleteNonExistentVirtualCluster() {
        VirtualClusterControlManager manager = newManager();

        assertThrows(InvalidRequestException.class,
            () -> manager.deleteVirtualCluster("ghost_cluster", MetadataVersion.IBP_4_4_IV0));
    }

    @Test
    public void testDeleteNonEmptyVirtualClusterFails() {
        VirtualClusterControlManager manager = newManagerWithTopics("my-topic");
        RecordTestUtils.replayAll(manager,
            manager.createVirtualCluster("vc", MetadataVersion.IBP_4_4_IV0).records());

        // Add one of each resource type
        RecordTestUtils.replayAll(manager, manager.alterVirtualCluster(
            alterRequest("vc", (byte) 0, "alice", null, (byte) 0),
            MetadataVersion.IBP_4_4_IV0).records());
        RecordTestUtils.replayAll(manager, manager.alterVirtualCluster(
            alterRequest("vc", (byte) 1, "client-1", null, (byte) 0),
            MetadataVersion.IBP_4_4_IV0).records());
        RecordTestUtils.replayAll(manager, manager.alterVirtualCluster(
            alterRequest("vc", (byte) 3, "my-group", null, (byte) 0),
            MetadataVersion.IBP_4_4_IV0).records());
        RecordTestUtils.replayAll(manager, manager.alterVirtualCluster(
            alterRequest("vc", (byte) 4, "tx-1", null, (byte) 0),
            MetadataVersion.IBP_4_4_IV0).records());
        RecordTestUtils.replayAll(manager, manager.alterVirtualCluster(
            alterRequest("vc", (byte) 2, "my-topic", "link-1", (byte) 0),
            MetadataVersion.IBP_4_4_IV0).records());

        // Delete must fail while any resource is present
        InvalidRequestException ex = assertThrows(InvalidRequestException.class,
            () -> manager.deleteVirtualCluster("vc", MetadataVersion.IBP_4_4_IV0));
        assertTrue(ex.getMessage().contains("1 topic(s)"),   "message should mention topics");
        assertTrue(ex.getMessage().contains("1 user(s)"),    "message should mention users");
        assertTrue(ex.getMessage().contains("1 client(s)"),  "message should mention clients");
        assertTrue(ex.getMessage().contains("1 group(s)"),   "message should mention groups");
        assertTrue(ex.getMessage().contains("1 transactional-id(s)"), "message should mention transactional-ids");

        // Remove all resources one by one, delete must still fail until fully empty
        RecordTestUtils.replayAll(manager, manager.alterVirtualCluster(
            alterRequest("vc", (byte) 0, "alice", null, (byte) 1),
            MetadataVersion.IBP_4_4_IV0).records());
        assertThrows(InvalidRequestException.class,
            () -> manager.deleteVirtualCluster("vc", MetadataVersion.IBP_4_4_IV0));

        RecordTestUtils.replayAll(manager, manager.alterVirtualCluster(
            alterRequest("vc", (byte) 1, "client-1", null, (byte) 1),
            MetadataVersion.IBP_4_4_IV0).records());
        assertThrows(InvalidRequestException.class,
            () -> manager.deleteVirtualCluster("vc", MetadataVersion.IBP_4_4_IV0));

        RecordTestUtils.replayAll(manager, manager.alterVirtualCluster(
            alterRequest("vc", (byte) 3, "my-group", null, (byte) 1),
            MetadataVersion.IBP_4_4_IV0).records());
        assertThrows(InvalidRequestException.class,
            () -> manager.deleteVirtualCluster("vc", MetadataVersion.IBP_4_4_IV0));

        RecordTestUtils.replayAll(manager, manager.alterVirtualCluster(
            alterRequest("vc", (byte) 4, "tx-1", null, (byte) 1),
            MetadataVersion.IBP_4_4_IV0).records());
        assertThrows(InvalidRequestException.class,
            () -> manager.deleteVirtualCluster("vc", MetadataVersion.IBP_4_4_IV0));

        RecordTestUtils.replayAll(manager, manager.alterVirtualCluster(
            alterRequest("vc", (byte) 2, "my-topic", null, (byte) 1),
            MetadataVersion.IBP_4_4_IV0).records());

        // Now fully empty — delete must succeed
        ControllerResult<Void> result = manager.deleteVirtualCluster("vc", MetadataVersion.IBP_4_4_IV0);
        assertEquals(1, result.records().size());
        assertEquals(RemoveVirtualClusterRecord.class, result.records().get(0).message().getClass());
    }


    private static AlterableVirtualCluster alterRequest(String vcName,
                                                        byte resourceType,
                                                        String resourceName,
                                                        String linkName,
                                                        byte operation) {
        AlterVirtualClusterResource resource = new AlterVirtualClusterResource()
            .setResourceType(resourceType)
            .setResourceName(resourceName)
            .setLinkName(linkName)
            .setOperation(operation);
        return new AlterableVirtualCluster()
            .setName(vcName)
            .setResources(List.of(resource));
    }

    @Test
    public void testAlterVirtualClusterUnsupportedVersion() {
        VirtualClusterControlManager manager = newManager();
        AlterableVirtualCluster data = new AlterableVirtualCluster().setName("vc");

        assertThrows(UnsupportedVersionException.class,
            () -> manager.alterVirtualCluster(data, MetadataVersion.IBP_4_3_IV0));
    }

    @Test
    public void testAlterNonExistentVirtualCluster() {
        VirtualClusterControlManager manager = newManager();
        AlterableVirtualCluster data = new AlterableVirtualCluster().setName("ghost");

        assertThrows(InvalidRequestException.class,
            () -> manager.alterVirtualCluster(data, MetadataVersion.IBP_4_4_IV0));
    }

    @Test
    public void testAlterVirtualClusterAddUser() {
        VirtualClusterControlManager manager = newManager();
        RecordTestUtils.replayAll(manager,
            manager.createVirtualCluster("vc", MetadataVersion.IBP_4_4_IV0).records());

        AlterableVirtualCluster data = alterRequest("vc", (byte) 0, "alice", null, (byte) 0);
        ControllerResult<Void> result = manager.alterVirtualCluster(data, MetadataVersion.IBP_4_4_IV0);

        List<ApiMessageAndVersion> records = result.records();
        assertEquals(1, records.size());
        assertEquals(VirtualClusterChangeRecord.class, records.get(0).message().getClass());
        VirtualClusterChangeRecord changeRecord = (VirtualClusterChangeRecord) records.get(0).message();
        assertEquals("vc", changeRecord.name());
        assertEquals(List.of("alice"), changeRecord.addedUsers());
        assertTrue(changeRecord.removedUsers().isEmpty());

        RecordTestUtils.replayAll(manager, records);

        // After replay the user is visible via describe
        assertEquals(List.of("alice"), manager.describeVirtualCluster("vc", MetadataVersion.IBP_4_4_IV0).users());

        // Adding duplicate should fail
        assertThrows(InvalidRequestException.class,
            () -> manager.alterVirtualCluster(data, MetadataVersion.IBP_4_4_IV0));
    }

    @Test
    public void testAlterVirtualClusterRemoveUser() {
        VirtualClusterControlManager manager = newManager();
        RecordTestUtils.replayAll(manager,
            manager.createVirtualCluster("vc", MetadataVersion.IBP_4_4_IV0).records());
        // Add user first
        RecordTestUtils.replayAll(manager,
            manager.alterVirtualCluster(
                alterRequest("vc", (byte) 0, "alice", null, (byte) 0),
                MetadataVersion.IBP_4_4_IV0).records());

        // Now remove
        AlterableVirtualCluster removeData = alterRequest("vc", (byte) 0, "alice", null, (byte) 1);
        ControllerResult<Void> result = manager.alterVirtualCluster(removeData, MetadataVersion.IBP_4_4_IV0);
        assertEquals(1, result.records().size());
        VirtualClusterChangeRecord changeRecord = (VirtualClusterChangeRecord) result.records().get(0).message();
        assertEquals(List.of("alice"), changeRecord.removedUsers());
        assertTrue(changeRecord.addedUsers().isEmpty());

        RecordTestUtils.replayAll(manager, result.records());
        assertEquals(0, manager.describeVirtualCluster("vc", MetadataVersion.IBP_4_4_IV0).users().size());

        // Removing non-existent should fail
        assertThrows(InvalidRequestException.class,
            () -> manager.alterVirtualCluster(removeData, MetadataVersion.IBP_4_4_IV0));
    }

    @Test
    public void testAlterVirtualClusterAddTopic() {
        VirtualClusterControlManager manager = newManagerWithTopics("my-topic");
        RecordTestUtils.replayAll(manager,
            manager.createVirtualCluster("vc", MetadataVersion.IBP_4_4_IV0).records());

        AlterableVirtualCluster data = alterRequest("vc", (byte) 2, "my-topic", "link-1", (byte) 0);
        ControllerResult<Void> result = manager.alterVirtualCluster(data, MetadataVersion.IBP_4_4_IV0);
        assertEquals(1, result.records().size());
        VirtualClusterChangeRecord changeRecord = (VirtualClusterChangeRecord) result.records().get(0).message();
        assertEquals(1, changeRecord.addedTopics().size());
        assertEquals("my-topic", changeRecord.addedTopics().get(0).topicName());
        assertEquals("link-1", changeRecord.addedTopics().get(0).linkName());
        assertTrue(changeRecord.removedTopics().isEmpty());

        RecordTestUtils.replayAll(manager, result.records());
        VirtualClusterRecord described = manager.describeVirtualCluster("vc", MetadataVersion.IBP_4_4_IV0);
        assertEquals(1, described.topics().size());
        assertEquals("my-topic", described.topics().get(0).topicName());
        assertEquals("link-1", described.topics().get(0).linkName());
    }

    @Test
    public void testAlterVirtualClusterAddTopicRequiresLinkName() {
        VirtualClusterControlManager manager = newManagerWithTopics("my-topic");
        RecordTestUtils.replayAll(manager,
            manager.createVirtualCluster("vc", MetadataVersion.IBP_4_4_IV0).records());

        // null link name should fail
        AlterableVirtualCluster data = alterRequest("vc", (byte) 2, "my-topic", null, (byte) 0);
        assertThrows(InvalidRequestException.class,
            () -> manager.alterVirtualCluster(data, MetadataVersion.IBP_4_4_IV0));
    }

    @Test
    public void testAlterVirtualClusterAddTopicDuplicateLinkName() {
        VirtualClusterControlManager manager = newManagerWithTopics("topic-a", "topic-b");
        RecordTestUtils.replayAll(manager,
            manager.createVirtualCluster("vc", MetadataVersion.IBP_4_4_IV0).records());

        // Add topic-a with link name "link-1"
        RecordTestUtils.replayAll(manager,
            manager.alterVirtualCluster(
                alterRequest("vc", (byte) 2, "topic-a", "link-1", (byte) 0),
                MetadataVersion.IBP_4_4_IV0).records());

        // Attempt to add topic-b with the same link name "link-1" — must fail
        AlterableVirtualCluster data = alterRequest("vc", (byte) 2, "topic-b", "link-1", (byte) 0);
        assertThrows(InvalidRequestException.class,
            () -> manager.alterVirtualCluster(data, MetadataVersion.IBP_4_4_IV0));
    }

    @Test
    public void testAlterVirtualClusterAddTopicDoesNotExist() {
        // Manager with no known topics
        VirtualClusterControlManager manager = newManagerWithTopics();
        RecordTestUtils.replayAll(manager,
            manager.createVirtualCluster("vc", MetadataVersion.IBP_4_4_IV0).records());

        AlterableVirtualCluster data = alterRequest("vc", (byte) 2, "missing-topic", "link-1", (byte) 0);
        assertThrows(InvalidRequestException.class,
            () -> manager.alterVirtualCluster(data, MetadataVersion.IBP_4_4_IV0));
    }

    @Test
    public void testAlterVirtualClusterAddClient() {
        VirtualClusterControlManager manager = newManager();
        RecordTestUtils.replayAll(manager,
            manager.createVirtualCluster("vc", MetadataVersion.IBP_4_4_IV0).records());

        AlterableVirtualCluster data = alterRequest("vc", (byte) 1, "client-1", null, (byte) 0);
        ControllerResult<Void> result = manager.alterVirtualCluster(data, MetadataVersion.IBP_4_4_IV0);
        VirtualClusterChangeRecord changeRecord = (VirtualClusterChangeRecord) result.records().get(0).message();
        assertEquals(List.of("client-1"), changeRecord.addedClients());
        assertTrue(changeRecord.removedClients().isEmpty());

        RecordTestUtils.replayAll(manager, result.records());
        assertEquals(List.of("client-1"), manager.describeVirtualCluster("vc", MetadataVersion.IBP_4_4_IV0).clients());
    }

    @Test
    public void testAlterVirtualClusterAddGroup() {
        VirtualClusterControlManager manager = newManager();
        RecordTestUtils.replayAll(manager,
            manager.createVirtualCluster("vc", MetadataVersion.IBP_4_4_IV0).records());

        AlterableVirtualCluster data = alterRequest("vc", (byte) 3, "my-group", null, (byte) 0);
        ControllerResult<Void> result = manager.alterVirtualCluster(data, MetadataVersion.IBP_4_4_IV0);
        VirtualClusterChangeRecord changeRecord = (VirtualClusterChangeRecord) result.records().get(0).message();
        assertEquals(List.of("my-group"), changeRecord.addedGroups());
        assertTrue(changeRecord.removedGroups().isEmpty());

        RecordTestUtils.replayAll(manager, result.records());
        assertEquals(List.of("my-group"), manager.describeVirtualCluster("vc", MetadataVersion.IBP_4_4_IV0).groups());
    }

    @Test
    public void testAlterVirtualClusterAddTransactionalId() {
        VirtualClusterControlManager manager = newManager();
        RecordTestUtils.replayAll(manager,
            manager.createVirtualCluster("vc", MetadataVersion.IBP_4_4_IV0).records());

        AlterableVirtualCluster data = alterRequest("vc", (byte) 4, "tx-1", null, (byte) 0);
        ControllerResult<Void> result = manager.alterVirtualCluster(data, MetadataVersion.IBP_4_4_IV0);
        VirtualClusterChangeRecord changeRecord = (VirtualClusterChangeRecord) result.records().get(0).message();
        assertEquals(List.of("tx-1"), changeRecord.addedTransactionalIds());
        assertTrue(changeRecord.removedTransactionalIds().isEmpty());

        RecordTestUtils.replayAll(manager, result.records());
        assertEquals(List.of("tx-1"), manager.describeVirtualCluster("vc", MetadataVersion.IBP_4_4_IV0).transactionalIds());
    }

    @Test
    public void testListVirtualClusters() {
        VirtualClusterControlManager manager = newManager();

        // Create two VCs and replay
        RecordTestUtils.replayAll(manager,
            manager.createVirtualCluster("vc_a", MetadataVersion.IBP_4_4_IV0).records());
        RecordTestUtils.replayAll(manager,
            manager.createVirtualCluster("vc_b", MetadataVersion.IBP_4_4_IV0).records());

        List<String> names = manager.listVirtualClusters(MetadataVersion.IBP_4_4_IV0);
        assertEquals(2, names.size());
        assertTrue(names.contains("vc_a"));
        assertTrue(names.contains("vc_b"));
    }

    @Test
    public void testListVirtualClustersEmpty() {
        VirtualClusterControlManager manager = newManager();

        List<String> names = manager.listVirtualClusters(MetadataVersion.IBP_4_4_IV0);
        assertEquals(0, names.size());
    }

    @Test
    public void testListVirtualClustersUnsupportedVersion() {
        VirtualClusterControlManager manager = newManager();

        assertThrows(UnsupportedVersionException.class,
            () -> manager.listVirtualClusters(MetadataVersion.IBP_4_3_IV0));
    }

    // ---- describeVirtualCluster tests ----

    @Test
    public void testDescribeVirtualCluster() {
        VirtualClusterControlManager manager = newManagerWithTopics("my-topic");
        RecordTestUtils.replayAll(manager,
            manager.createVirtualCluster("vc", MetadataVersion.IBP_4_4_IV0).records());
        // Add a topic link
        RecordTestUtils.replayAll(manager,
            manager.alterVirtualCluster(
                alterRequest("vc", (byte) 2, "my-topic", "link-1", (byte) 0),
                MetadataVersion.IBP_4_4_IV0).records());
        // Add a user
        RecordTestUtils.replayAll(manager,
            manager.alterVirtualCluster(
                alterRequest("vc", (byte) 0, "alice", null, (byte) 0),
                MetadataVersion.IBP_4_4_IV0).records());

        VirtualClusterRecord record = manager.describeVirtualCluster("vc", MetadataVersion.IBP_4_4_IV0);

        assertEquals("vc", record.name());
        assertEquals(1, record.topics().size());
        assertEquals("my-topic", record.topics().get(0).topicName());
        assertEquals("link-1", record.topics().get(0).linkName());
        assertEquals(1, record.users().size());
        assertEquals("alice", record.users().get(0));
    }

    @Test
    public void testDescribeNonExistentVirtualCluster() {
        VirtualClusterControlManager manager = newManager();

        assertThrows(InvalidRequestException.class,
            () -> manager.describeVirtualCluster("ghost", MetadataVersion.IBP_4_4_IV0));
    }

    @Test
    public void testDescribeVirtualClusterUnsupportedVersion() {
        VirtualClusterControlManager manager = newManager();

        assertThrows(UnsupportedVersionException.class,
            () -> manager.describeVirtualCluster("vc", MetadataVersion.IBP_4_3_IV0));
    }

    // ---- cross-VC uniqueness tests ----

    @Test
    public void testUserCannotBelongToTwoVirtualClusters() {
        VirtualClusterControlManager manager = newManager();
        RecordTestUtils.replayAll(manager,
            manager.createVirtualCluster("vc1", MetadataVersion.IBP_4_4_IV0).records());
        RecordTestUtils.replayAll(manager,
            manager.createVirtualCluster("vc2", MetadataVersion.IBP_4_4_IV0).records());

        RecordTestUtils.replayAll(manager,
            manager.alterVirtualCluster(
                alterRequest("vc1", (byte) 0, "alice", null, (byte) 0),
                MetadataVersion.IBP_4_4_IV0).records());

        assertThrows(InvalidRequestException.class,
            () -> manager.alterVirtualCluster(
                alterRequest("vc2", (byte) 0, "alice", null, (byte) 0),
                MetadataVersion.IBP_4_4_IV0));
    }

    @Test
    public void testClientCannotBelongToTwoVirtualClusters() {
        VirtualClusterControlManager manager = newManager();
        RecordTestUtils.replayAll(manager,
            manager.createVirtualCluster("vc1", MetadataVersion.IBP_4_4_IV0).records());
        RecordTestUtils.replayAll(manager,
            manager.createVirtualCluster("vc2", MetadataVersion.IBP_4_4_IV0).records());

        RecordTestUtils.replayAll(manager,
            manager.alterVirtualCluster(
                alterRequest("vc1", (byte) 1, "client-1", null, (byte) 0),
                MetadataVersion.IBP_4_4_IV0).records());

        assertThrows(InvalidRequestException.class,
            () -> manager.alterVirtualCluster(
                alterRequest("vc2", (byte) 1, "client-1", null, (byte) 0),
                MetadataVersion.IBP_4_4_IV0));
    }

    @Test
    public void testGroupCannotBelongToTwoVirtualClusters() {
        VirtualClusterControlManager manager = newManager();
        RecordTestUtils.replayAll(manager,
            manager.createVirtualCluster("vc1", MetadataVersion.IBP_4_4_IV0).records());
        RecordTestUtils.replayAll(manager,
            manager.createVirtualCluster("vc2", MetadataVersion.IBP_4_4_IV0).records());

        RecordTestUtils.replayAll(manager,
            manager.alterVirtualCluster(
                alterRequest("vc1", (byte) 3, "my-group", null, (byte) 0),
                MetadataVersion.IBP_4_4_IV0).records());

        assertThrows(InvalidRequestException.class,
            () -> manager.alterVirtualCluster(
                alterRequest("vc2", (byte) 3, "my-group", null, (byte) 0),
                MetadataVersion.IBP_4_4_IV0));
    }

    @Test
    public void testTransactionalIdCannotBelongToTwoVirtualClusters() {
        VirtualClusterControlManager manager = newManager();
        RecordTestUtils.replayAll(manager,
            manager.createVirtualCluster("vc1", MetadataVersion.IBP_4_4_IV0).records());
        RecordTestUtils.replayAll(manager,
            manager.createVirtualCluster("vc2", MetadataVersion.IBP_4_4_IV0).records());

        RecordTestUtils.replayAll(manager,
            manager.alterVirtualCluster(
                alterRequest("vc1", (byte) 4, "tx-1", null, (byte) 0),
                MetadataVersion.IBP_4_4_IV0).records());

        assertThrows(InvalidRequestException.class,
            () -> manager.alterVirtualCluster(
                alterRequest("vc2", (byte) 4, "tx-1", null, (byte) 0),
                MetadataVersion.IBP_4_4_IV0));
    }

    @Test
    public void testAlterEmitsChangeRecordNotFullSnapshot() {
        // Core regression guard: alter must emit VirtualClusterChangeRecord, not VirtualClusterRecord.
        VirtualClusterControlManager manager = newManager();
        RecordTestUtils.replayAll(manager,
            manager.createVirtualCluster("vc", MetadataVersion.IBP_4_4_IV0).records());

        ControllerResult<Void> result = manager.alterVirtualCluster(
            alterRequest("vc", (byte) 0, "alice", null, (byte) 0),
            MetadataVersion.IBP_4_4_IV0);

        assertEquals(1, result.records().size());
        assertEquals(VirtualClusterChangeRecord.class, result.records().get(0).message().getClass());
    }

    @Test
    public void testAlterBatchMultipleOpsInOneRecord() {
        // A single AlterableVirtualCluster with multiple resource ops produces ONE change record
        // containing all changes, not one record per op.
        VirtualClusterControlManager manager = newManager();
        RecordTestUtils.replayAll(manager,
            manager.createVirtualCluster("vc", MetadataVersion.IBP_4_4_IV0).records());
        // Pre-populate alice so we can remove her in the same batch
        RecordTestUtils.replayAll(manager, manager.alterVirtualCluster(
            alterRequest("vc", (byte) 0, "alice", null, (byte) 0),
            MetadataVersion.IBP_4_4_IV0).records());

        // Build a single alter with: add bob, remove alice, add client-1
        AlterableVirtualCluster batch = new AlterableVirtualCluster().setName("vc")
            .setResources(List.of(
                new AlterVirtualClusterResource().setResourceType((byte) 0).setResourceName("bob").setOperation((byte) 0),
                new AlterVirtualClusterResource().setResourceType((byte) 0).setResourceName("alice").setOperation((byte) 1),
                new AlterVirtualClusterResource().setResourceType((byte) 1).setResourceName("client-1").setOperation((byte) 0)
            ));

        ControllerResult<Void> result = manager.alterVirtualCluster(batch, MetadataVersion.IBP_4_4_IV0);

        // Must be exactly one record
        assertEquals(1, result.records().size());
        VirtualClusterChangeRecord changeRecord = (VirtualClusterChangeRecord) result.records().get(0).message();
        assertEquals(List.of("bob"), changeRecord.addedUsers());
        assertEquals(List.of("alice"), changeRecord.removedUsers());
        assertEquals(List.of("client-1"), changeRecord.addedClients());
        assertTrue(changeRecord.removedClients().isEmpty());

        // After replay the state is correct
        RecordTestUtils.replayAll(manager, result.records());
        VirtualClusterRecord described = manager.describeVirtualCluster("vc", MetadataVersion.IBP_4_4_IV0);
        assertTrue(described.users().contains("bob"));
        assertTrue(described.clients().contains("client-1"));
        assertTrue(!described.users().contains("alice"));
    }

    @Test
    public void testReplayChangeRecordUpdatesState() {
        // Direct replay of a VirtualClusterChangeRecord updates in-memory state correctly.
        VirtualClusterControlManager manager = newManager();
        RecordTestUtils.replayAll(manager,
            manager.createVirtualCluster("vc", MetadataVersion.IBP_4_4_IV0).records());

        VirtualClusterChangeRecord change = new VirtualClusterChangeRecord()
            .setName("vc")
            .setAddedUsers(List.of("alice", "bob"))
            .setRemovedUsers(List.of())
            .setAddedClients(List.of())
            .setRemovedClients(List.of())
            .setAddedTopics(List.of())
            .setRemovedTopics(List.of())
            .setAddedGroups(List.of())
            .setRemovedGroups(List.of())
            .setAddedTransactionalIds(List.of())
            .setRemovedTransactionalIds(List.of());

        manager.replay(change);

        VirtualClusterRecord described = manager.describeVirtualCluster("vc", MetadataVersion.IBP_4_4_IV0);
        assertEquals(2, described.users().size());
        assertTrue(described.users().contains("alice"));
        assertTrue(described.users().contains("bob"));

        // Now replay a removal
        VirtualClusterChangeRecord removal = new VirtualClusterChangeRecord()
            .setName("vc")
            .setAddedUsers(List.of())
            .setRemovedUsers(List.of("alice"))
            .setAddedClients(List.of())
            .setRemovedClients(List.of())
            .setAddedTopics(List.of())
            .setRemovedTopics(List.of())
            .setAddedGroups(List.of())
            .setRemovedGroups(List.of())
            .setAddedTransactionalIds(List.of())
            .setRemovedTransactionalIds(List.of());

        manager.replay(removal);

        VirtualClusterRecord afterRemoval = manager.describeVirtualCluster("vc", MetadataVersion.IBP_4_4_IV0);
        assertEquals(1, afterRemoval.users().size());
        assertEquals("bob", afterRemoval.users().get(0));
    }

    @Test
    public void testResourceCanBeReassignedAfterRemoval() {
        VirtualClusterControlManager manager = newManager();
        RecordTestUtils.replayAll(manager,
            manager.createVirtualCluster("vc1", MetadataVersion.IBP_4_4_IV0).records());
        RecordTestUtils.replayAll(manager,
            manager.createVirtualCluster("vc2", MetadataVersion.IBP_4_4_IV0).records());

        // Add alice to vc1, then remove her
        RecordTestUtils.replayAll(manager,
            manager.alterVirtualCluster(
                alterRequest("vc1", (byte) 0, "alice", null, (byte) 0),
                MetadataVersion.IBP_4_4_IV0).records());
        RecordTestUtils.replayAll(manager,
            manager.alterVirtualCluster(
                alterRequest("vc1", (byte) 0, "alice", null, (byte) 1),
                MetadataVersion.IBP_4_4_IV0).records());

        // Now alice can be added to vc2
        ControllerResult<Void> result = manager.alterVirtualCluster(
            alterRequest("vc2", (byte) 0, "alice", null, (byte) 0),
            MetadataVersion.IBP_4_4_IV0);
        VirtualClusterChangeRecord changeRecord = (VirtualClusterChangeRecord) result.records().get(0).message();
        assertEquals(List.of("alice"), changeRecord.addedUsers());
        assertTrue(changeRecord.removedUsers().isEmpty());

        RecordTestUtils.replayAll(manager, result.records());
        assertEquals(List.of("alice"), manager.describeVirtualCluster("vc2", MetadataVersion.IBP_4_4_IV0).users());
    }
}
