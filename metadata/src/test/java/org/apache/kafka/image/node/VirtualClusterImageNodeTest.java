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

import org.apache.kafka.common.metadata.VirtualClusterRecord;
import org.apache.kafka.image.VirtualClusterDelta;
import org.apache.kafka.image.VirtualClusterImage;
import org.apache.kafka.image.node.printer.NodeStringifier;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 40)
public class VirtualClusterImageNodeTest {

    private static VirtualClusterImage emptyImage(String name) {
        return new VirtualClusterImage(name, List.of(), List.of(), List.of(), List.of(), List.of());
    }

    private static final VirtualClusterImageNode NODE =
        new VirtualClusterImageNode(emptyImage("test_cluster"));

    @Test
    public void testChildNames() {
        assertEquals(
            List.of("name", "topics", "users", "clients", "groups", "transactionalIds"),
            List.copyOf(NODE.childNames()));
    }

    @Test
    public void testNameChild() {
        MetadataNode child = NODE.child("name");
        assertNotNull(child);
        assertEquals(MetadataLeafNode.class, child.getClass());
        NodeStringifier stringifier = new NodeStringifier();
        child.print(stringifier);
        assertEquals("test_cluster", stringifier.toString());
    }

    @Test
    public void testTopicsChild() {
        MetadataNode child = NODE.child("topics");
        assertNotNull(child);
        assertEquals(MetadataLeafNode.class, child.getClass());
    }

    @Test
    public void testUsersChild() {
        MetadataNode child = NODE.child("users");
        assertNotNull(child);
        assertEquals(MetadataLeafNode.class, child.getClass());
    }

    @Test
    public void testClientsChild() {
        MetadataNode child = NODE.child("clients");
        assertNotNull(child);
        assertEquals(MetadataLeafNode.class, child.getClass());
    }

    @Test
    public void testGroupsChild() {
        MetadataNode child = NODE.child("groups");
        assertNotNull(child);
        assertEquals(MetadataLeafNode.class, child.getClass());
    }

    @Test
    public void testTransactionalIdsChild() {
        MetadataNode child = NODE.child("transactionalIds");
        assertNotNull(child);
        assertEquals(MetadataLeafNode.class, child.getClass());
    }

    @Test
    public void testUnknownChild() {
        assertNull(NODE.child("unknown"));
    }

    // -------------------------------------------------------------------------
    // VirtualClusterDelta tests
    // -------------------------------------------------------------------------

    private static VirtualClusterRecord recordWith(
        String name,
        List<VirtualClusterRecord.VirtualClusterTopicLink> topics,
        List<String> users,
        List<String> clients,
        List<String> groups,
        List<String> transactionalIds
    ) {
        return new VirtualClusterRecord()
            .setName(name)
            .setTopics(topics)
            .setUsers(users)
            .setClients(clients)
            .setGroups(groups)
            .setTransactionalIds(transactionalIds);
    }

    private static VirtualClusterRecord.VirtualClusterTopicLink topicLink(String topicName, String linkName) {
        return new VirtualClusterRecord.VirtualClusterTopicLink()
            .setTopicName(topicName)
            .setLinkName(linkName);
    }

    @Test
    public void testDeltaAddResources() {
        VirtualClusterImage base = emptyImage("vc1");
        VirtualClusterDelta delta = new VirtualClusterDelta(base);

        VirtualClusterRecord record = recordWith(
            "vc1",
            List.of(topicLink("physical-topic", "link1")),
            List.of("alice"),
            List.of("client1"),
            List.of("group1"),
            List.of("txn1")
        );
        delta.replay(record);

        assertEquals(List.of(new VirtualClusterImage.TopicLink("physical-topic", "link1")), delta.addedTopicLinks());
        assertTrue(delta.removedTopicLinks().isEmpty());
        assertEquals(List.of("alice"), delta.addedUsers());
        assertTrue(delta.removedUsers().isEmpty());
        assertEquals(List.of("client1"), delta.addedClients());
        assertTrue(delta.removedClients().isEmpty());
        assertEquals(List.of("group1"), delta.addedGroups());
        assertTrue(delta.removedGroups().isEmpty());
        assertEquals(List.of("txn1"), delta.addedTransactionalIds());
        assertTrue(delta.removedTransactionalIds().isEmpty());
    }

    @Test
    public void testDeltaRemoveResources() {
        VirtualClusterImage base = new VirtualClusterImage(
            "vc1",
            List.of(new VirtualClusterImage.TopicLink("physical-topic", "link1")),
            List.of("alice"),
            List.of("client1"),
            List.of("group1"),
            List.of("txn1")
        );
        VirtualClusterDelta delta = new VirtualClusterDelta(base);

        // New record has everything removed
        VirtualClusterRecord record = recordWith("vc1",
            Collections.emptyList(), Collections.emptyList(),
            Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
        delta.replay(record);

        assertTrue(delta.addedTopicLinks().isEmpty());
        assertEquals(List.of(new VirtualClusterImage.TopicLink("physical-topic", "link1")), delta.removedTopicLinks());
        assertTrue(delta.addedUsers().isEmpty());
        assertEquals(List.of("alice"), delta.removedUsers());
        assertTrue(delta.addedClients().isEmpty());
        assertEquals(List.of("client1"), delta.removedClients());
        assertTrue(delta.addedGroups().isEmpty());
        assertEquals(List.of("group1"), delta.removedGroups());
        assertTrue(delta.addedTransactionalIds().isEmpty());
        assertEquals(List.of("txn1"), delta.removedTransactionalIds());
    }

    @Test
    public void testDeltaApplyProducesCorrectImage() {
        VirtualClusterImage base = new VirtualClusterImage(
            "vc1",
            List.of(new VirtualClusterImage.TopicLink("old-topic", "old-link")),
            List.of("alice"),
            List.of(),
            List.of(),
            List.of()
        );
        VirtualClusterDelta delta = new VirtualClusterDelta(base);

        // Replace old-topic with new-topic, keep alice, add bob
        VirtualClusterRecord record = recordWith(
            "vc1",
            List.of(topicLink("new-topic", "new-link")),
            List.of("alice", "bob"),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList()
        );
        delta.replay(record);

        VirtualClusterImage result = delta.apply();
        assertEquals("vc1", result.name());
        assertEquals(List.of(new VirtualClusterImage.TopicLink("new-topic", "new-link")), result.topics());
        assertEquals(List.of("alice", "bob"), result.users());
        assertTrue(result.clients().isEmpty());
    }

    @Test
    public void testDeltaMultipleReplaysAccumulateDiff() {
        VirtualClusterImage base = emptyImage("vc1");
        VirtualClusterDelta delta = new VirtualClusterDelta(base);

        // First replay: add alice
        delta.replay(recordWith("vc1", Collections.emptyList(),
            List.of("alice"), Collections.emptyList(), Collections.emptyList(), Collections.emptyList()));

        // Second replay: add bob (alice stays)
        delta.replay(recordWith("vc1", Collections.emptyList(),
            List.of("alice", "bob"), Collections.emptyList(), Collections.emptyList(), Collections.emptyList()));

        VirtualClusterImage result = delta.apply();
        assertTrue(result.users().contains("alice"));
        assertTrue(result.users().contains("bob"));
    }
}
