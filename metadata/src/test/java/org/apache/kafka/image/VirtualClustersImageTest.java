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
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.image.writer.UnwritableMetadataException;
import org.apache.kafka.server.common.MetadataVersion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 40)
public class VirtualClustersImageTest {

    private static VirtualClusterImage emptyClusterImage(String name) {
        return new VirtualClusterImage(name, List.of(), List.of(), List.of(), List.of(), List.of());
    }

    private static VirtualClustersImage imageWithOneCluster(String name) {
        return new VirtualClustersImage(Map.of(name, emptyClusterImage(name)));
    }

    @Test
    public void testWriteWithSupportedVersion() {
        VirtualClustersImage image = imageWithOneCluster("test_cluster");
        RecordListWriter writer = new RecordListWriter();
        image.write(writer, new ImageWriterOptions.Builder(MetadataVersion.IBP_4_4_IV0).build());

        assertEquals(1, writer.records().size());
        assertEquals(VirtualClusterRecord.class, writer.records().get(0).message().getClass());
        assertEquals("test_cluster", ((VirtualClusterRecord) writer.records().get(0).message()).name());
    }

    @Test
    public void testWriteWithUnsupportedVersionTriggersLossHandler() {
        VirtualClustersImage image = imageWithOneCluster("test_cluster");
        RecordListWriter writer = new RecordListWriter();

        List<UnwritableMetadataException> losses = new ArrayList<>();
        ImageWriterOptions options = new ImageWriterOptions.Builder(MetadataVersion.IBP_4_3_IV0)
            .setLossHandler(losses::add)
            .build();

        image.write(writer, options);

        assertTrue(writer.records().isEmpty(), "No records should be written for unsupported version");
        assertEquals(1, losses.size());
        assertTrue(losses.get(0).getMessage().contains("test_cluster"));
    }

    @Test
    public void testEmptyImageWriteProducesNoRecords() {
        RecordListWriter writer = new RecordListWriter();
        VirtualClustersImage.EMPTY.write(writer,
            new ImageWriterOptions.Builder(MetadataVersion.IBP_4_3_IV0).build());

        assertTrue(writer.records().isEmpty());
    }

    @Test
    public void testFindVcForUserFound() {
        VirtualClusterImage vc = new VirtualClusterImage("vc1", List.of(), List.of("alice", "bob"), List.of(), List.of(), List.of());
        VirtualClustersImage image = new VirtualClustersImage(Map.of("vc1", vc));

        Optional<VirtualClusterImage> result = image.findVcForUser("alice");

        assertTrue(result.isPresent());
        assertEquals("vc1", result.get().name());
    }

    @Test
    public void testFindVcForUserNotFound() {
        VirtualClusterImage vc = new VirtualClusterImage("vc1", List.of(), List.of("alice"), List.of(), List.of(), List.of());
        VirtualClustersImage image = new VirtualClustersImage(Map.of("vc1", vc));

        Optional<VirtualClusterImage> result = image.findVcForUser("charlie");

        assertFalse(result.isPresent());
    }
}
