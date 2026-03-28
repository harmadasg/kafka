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

import java.util.List;

/**
 * A description of a virtual cluster returned by
 * {@link Admin#describeVirtualCluster(String, DescribeVirtualClusterOptions)}.
 */
public class VirtualClusterDescription {

    /**
     * A topic link within a virtual cluster: maps a link name (the name visible inside the
     * virtual cluster) to the physical topic name on the cluster.
     */
    public static class TopicLink {
        private final String linkName;
        private final String topicName;

        public TopicLink(String linkName, String topicName) {
            this.linkName = linkName;
            this.topicName = topicName;
        }

        /** The link name within the virtual cluster. */
        public String linkName() {
            return linkName;
        }

        /** The physical topic name on the cluster. */
        public String topicName() {
            return topicName;
        }

        @Override
        public String toString() {
            return "TopicLink(linkName=" + linkName + ", topicName=" + topicName + ")";
        }
    }

    private final String name;
    private final List<TopicLink> topicLinks;
    private final List<String> userLinks;
    private final List<String> clientLinks;
    private final List<String> groupLinks;
    private final List<String> transactionalIdLinks;

    public VirtualClusterDescription(
        String name,
        List<TopicLink> topicLinks,
        List<String> userLinks,
        List<String> clientLinks,
        List<String> groupLinks,
        List<String> transactionalIdLinks
    ) {
        this.name = name;
        this.topicLinks = topicLinks;
        this.userLinks = userLinks;
        this.clientLinks = clientLinks;
        this.groupLinks = groupLinks;
        this.transactionalIdLinks = transactionalIdLinks;
    }

    public String name() {
        return name;
    }

    public List<TopicLink> topicLinks() {
        return topicLinks;
    }

    public List<String> userLinks() {
        return userLinks;
    }

    public List<String> clientLinks() {
        return clientLinks;
    }

    public List<String> groupLinks() {
        return groupLinks;
    }

    public List<String> transactionalIdLinks() {
        return transactionalIdLinks;
    }

    @Override
    public String toString() {
        return "VirtualClusterDescription(" +
            "name=" + name +
            ", topicLinks=" + topicLinks +
            ", userLinks=" + userLinks +
            ", clientLinks=" + clientLinks +
            ", groupLinks=" + groupLinks +
            ", transactionalIdLinks=" + transactionalIdLinks +
            ")";
    }
}
