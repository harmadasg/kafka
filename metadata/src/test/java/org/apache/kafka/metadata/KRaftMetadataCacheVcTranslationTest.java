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
package org.apache.kafka.metadata;

import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.VirtualClusterImage;
import org.apache.kafka.image.VirtualClustersImage;
import org.apache.kafka.server.common.KRaftVersion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(value = 40)
public class KRaftMetadataCacheVcTranslationTest {

    private static KRaftMetadataCache cacheWithVc(VirtualClustersImage vcImage) {
        KRaftMetadataCache cache = new KRaftMetadataCache(0, () -> KRaftVersion.KRAFT_VERSION_0);
        MetadataImage image = new MetadataImage(
            MetadataImage.EMPTY.provenance(),
            MetadataImage.EMPTY.features(),
            MetadataImage.EMPTY.cluster(),
            MetadataImage.EMPTY.topics(),
            MetadataImage.EMPTY.configs(),
            MetadataImage.EMPTY.clientQuotas(),
            MetadataImage.EMPTY.producerIds(),
            MetadataImage.EMPTY.acls(),
            MetadataImage.EMPTY.scram(),
            MetadataImage.EMPTY.delegationTokens(),
            vcImage
        );
        cache.setImage(image);
        return cache;
    }

    private static VirtualClustersImage vcImageWithLink(String vcName, String user,
                                                        String linkName, String physicalName) {
        VirtualClusterImage.TopicLink link = new VirtualClusterImage.TopicLink(physicalName, linkName);
        VirtualClusterImage vc = new VirtualClusterImage(vcName, List.of(link), List.of(user),
            List.of(), List.of(), List.of());
        return new VirtualClustersImage(Map.of(vcName, vc));
    }

    // ---- resolveTopicName ----

    @Test
    public void resolveTopicName_noVcForPrincipal_returnsInput() {
        KRaftMetadataCache cache = cacheWithVc(VirtualClustersImage.EMPTY);
        assertEquals("my-topic", cache.resolveTopicName("alice", "my-topic"));
    }

    @Test
    public void resolveTopicName_principalInVcWithMatchingLink_returnsPhysical() {
        KRaftMetadataCache cache = cacheWithVc(vcImageWithLink("vc1", "alice", "link-a", "uuid-prefix.my-topic"));
        assertEquals("uuid-prefix.my-topic", cache.resolveTopicName("alice", "link-a"));
    }

    @Test
    public void resolveTopicName_principalInVcNoMatchingLink_returnsInput() {
        KRaftMetadataCache cache = cacheWithVc(vcImageWithLink("vc1", "alice", "link-a", "uuid-prefix.my-topic"));
        assertEquals("other-topic", cache.resolveTopicName("alice", "other-topic"));
    }

    @Test
    public void resolveTopicName_principalNotInVcIgnoresOtherVcLinks_returnsInput() {
        KRaftMetadataCache cache = cacheWithVc(vcImageWithLink("vc1", "alice", "link-a", "uuid-prefix.my-topic"));
        // "bob" is not in any VC — must not get alice's mapping
        assertEquals("link-a", cache.resolveTopicName("bob", "link-a"));
    }

    // ---- linkNameForTopic ----

    @Test
    public void linkNameForTopic_noVcForPrincipal_returnsInput() {
        KRaftMetadataCache cache = cacheWithVc(VirtualClustersImage.EMPTY);
        assertEquals("uuid-prefix.my-topic", cache.linkNameForTopic("alice", "uuid-prefix.my-topic"));
    }

    @Test
    public void linkNameForTopic_principalInVcWithMatchingPhysical_returnsLinkName() {
        KRaftMetadataCache cache = cacheWithVc(vcImageWithLink("vc1", "alice", "link-a", "uuid-prefix.my-topic"));
        assertEquals("link-a", cache.linkNameForTopic("alice", "uuid-prefix.my-topic"));
    }

    @Test
    public void linkNameForTopic_principalInVcNoMatchingPhysical_returnsInput() {
        KRaftMetadataCache cache = cacheWithVc(vcImageWithLink("vc1", "alice", "link-a", "uuid-prefix.my-topic"));
        assertEquals("other-physical-topic", cache.linkNameForTopic("alice", "other-physical-topic"));
    }

    @Test
    public void linkNameForTopic_principalNotInVcIgnoresOtherVcLinks_returnsInput() {
        KRaftMetadataCache cache = cacheWithVc(vcImageWithLink("vc1", "alice", "link-a", "uuid-prefix.my-topic"));
        // "bob" not in any VC — must not get alice's reverse mapping
        assertEquals("uuid-prefix.my-topic", cache.linkNameForTopic("bob", "uuid-prefix.my-topic"));
    }
}
