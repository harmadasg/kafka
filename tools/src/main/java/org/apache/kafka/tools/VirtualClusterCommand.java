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
package org.apache.kafka.tools;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterVirtualClusterOptions;
import org.apache.kafka.clients.admin.AlterVirtualClustersResult;
import org.apache.kafka.clients.admin.CreateVirtualClusterOptions;
import org.apache.kafka.clients.admin.CreateVirtualClustersResult;
import org.apache.kafka.clients.admin.DeleteVirtualClusterOptions;
import org.apache.kafka.clients.admin.DeleteVirtualClustersResult;
import org.apache.kafka.clients.admin.DescribeVirtualClusterOptions;
import org.apache.kafka.clients.admin.DescribeVirtualClustersResult;
import org.apache.kafka.clients.admin.ListVirtualClustersResult;
import org.apache.kafka.clients.admin.NewVirtualCluster;
import org.apache.kafka.clients.admin.VirtualClusterAlteration;
import org.apache.kafka.clients.admin.VirtualClusterDescription;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import joptsimple.NonOptionArgumentSpec;
import joptsimple.OptionSpec;

public class VirtualClusterCommand {
    public static void main(String... args) {
        Exit.exit(mainNoExit(args));
    }

    static int mainNoExit(String... args) {
        try {
            execute(args);
            return 0;
        } catch (TerseException e) {
            System.err.println(e.getMessage());
            return 1;
        } catch (Throwable e) {
            System.err.println(e.getMessage());
            System.err.println(Utils.stackTrace(e));
            return 1;
        }
    }

    static void execute(String... args) throws Exception {
        VirtualClusterCommandOptions opts = new VirtualClusterCommandOptions(args);
        CommandLineUtils.maybePrintHelpOrVersion(opts, "This tool helps to create, delete, alter, list and describe virtual clusters.\n\n" +
                "Usage: kafka-virtual-clusters.sh --bootstrap-server <host:port> <action> [options]\n\n" +
                "Actions: create, delete, alter, list, describe\n\n" +
                "alter options:\n" +
                "  --add | --remove\n" +
                "  --topic <name> [--link <link-name>]\n" +
                "  --user <name>\n" +
                "  --client <name>\n" +
                "  --group <name>\n" +
                "  --transactional-id <name>");

        opts.checkArgs();

        try (Admin adminClient = createAdminClient(opts)) {
            switch (opts.action()) {
                case "create":   createVirtualCluster(adminClient, opts);   break;
                case "delete":   deleteVirtualCluster(adminClient, opts);   break;
                case "alter":    alterVirtualCluster(adminClient, opts);    break;
                case "list":     listVirtualClusters(adminClient);          break;
                case "describe": describeVirtualCluster(adminClient, opts); break;
            }
        }
    }

    public static void createVirtualCluster(Admin adminClient, VirtualClusterCommandOptions opts) throws ExecutionException, InterruptedException {
        String name = opts.virtualClusterName();
        System.out.println("Creating virtual cluster: " + name);
        CreateVirtualClustersResult result = adminClient.createVirtualClusters(
                Collections.singleton(new NewVirtualCluster(name)), new CreateVirtualClusterOptions());
        result.values().get(name).get();
        System.out.println("Successfully created virtual cluster: " + name);
    }

    public static void deleteVirtualCluster(Admin adminClient, VirtualClusterCommandOptions opts) throws ExecutionException, InterruptedException {
        String name = opts.virtualClusterName();
        System.out.println("Deleting virtual cluster: " + name);
        DeleteVirtualClustersResult result = adminClient.deleteVirtualClusters(
                Collections.singleton(name), new DeleteVirtualClusterOptions());
        result.values().get(name).get();
        System.out.println("Successfully deleted virtual cluster: " + name);
    }

    public static void alterVirtualCluster(Admin adminClient, VirtualClusterCommandOptions opts) throws ExecutionException, InterruptedException {
        String name = opts.virtualClusterName();
        VirtualClusterAlteration.ResourceChangeType changeType = opts.isAddOpt()
                ? VirtualClusterAlteration.ResourceChangeType.ADD
                : VirtualClusterAlteration.ResourceChangeType.REMOVE;

        VirtualClusterAlteration.VirtualClusterEntityChange change = opts.resolveChange(changeType);

        String opLabel = changeType == VirtualClusterAlteration.ResourceChangeType.ADD ? "add" : "remove";
        System.out.println("Altering virtual cluster '" + name + "': " + opLabel + " "
                + change.resourceType().name().toLowerCase() + " '" + change.resourceName() + "'");

        VirtualClusterAlteration alteration = new VirtualClusterAlteration(name, Collections.singletonList(change));
        AlterVirtualClustersResult result = adminClient.alterVirtualClusters(
                Collections.singleton(alteration), new AlterVirtualClusterOptions());
        result.values().get(name).get();
        System.out.println("Successfully altered virtual cluster: " + name);
    }

    public static void listVirtualClusters(Admin adminClient) throws ExecutionException, InterruptedException {
        ListVirtualClustersResult result = adminClient.listVirtualClusters();
        List<String> names = result.listing().get();
        if (names.isEmpty()) {
            System.out.println("No virtual clusters found.");
        } else {
            for (String name : names) {
                System.out.println(name);
            }
        }
    }

    public static void describeVirtualCluster(Admin adminClient, VirtualClusterCommandOptions opts) throws ExecutionException, InterruptedException {
        String name = opts.virtualClusterName();
        DescribeVirtualClustersResult result = adminClient.describeVirtualClusters(
                Collections.singleton(name), new DescribeVirtualClusterOptions());
        VirtualClusterDescription desc = result.values().get(name).get();

        System.out.println("Virtual cluster: " + desc.name());

        List<VirtualClusterDescription.TopicLink> topics = desc.topicLinks();
        if (topics.isEmpty()) {
            System.out.println("  Topics: (none)");
        } else {
            System.out.println("  Topics:");
            for (VirtualClusterDescription.TopicLink t : topics) {
                System.out.println("    link=" + t.linkName() + " -> physical=" + t.topicName());
            }
        }

        printList("  Users", desc.userLinks());
        printList("  Clients", desc.clientLinks());
        printList("  Groups", desc.groupLinks());
        printList("  TransactionalIds", desc.transactionalIdLinks());
    }

    private static void printList(String label, List<String> items) {
        if (items.isEmpty()) {
            System.out.println(label + ": (none)");
        } else {
            System.out.println(label + ":");
            for (String item : items) {
                System.out.println("    " + item);
            }
        }
    }

    private static Admin createAdminClient(VirtualClusterCommandOptions opts) throws IOException {
        Properties props = new Properties();
        if (opts.options.has(opts.commandConfigOpt)) {
            props = Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt));
        }
        props.put("bootstrap.servers", opts.options.valueOf(opts.bootstrapServerOpt));
        return Admin.create(props);
    }

    static class VirtualClusterCommandOptions extends CommandDefaultOptions {
        public final OptionSpec<String> bootstrapServerOpt;
        public final OptionSpec<String> commandConfigOpt;
        public final OptionSpec<Void>   addOpt;
        public final OptionSpec<Void>   removeOpt;
        public final OptionSpec<String> virtualClusterOpt;
        public final OptionSpec<String> topicOpt;
        public final OptionSpec<String> userOpt;
        public final OptionSpec<String> clientOpt;
        public final OptionSpec<String> groupOpt;
        public final OptionSpec<String> transactionalIdOpt;
        public final OptionSpec<String> linkOpt;
        public final NonOptionArgumentSpec<String> actionArg;

        public VirtualClusterCommandOptions(String[] args) {
            super(args);

            this.bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED: server(s) to use for bootstrapping.")
                    .withRequiredArg()
                    .ofType(String.class);

            this.commandConfigOpt = parser.accepts("command-config", "A property file containing configs to be passed to Admin Client.")
                    .withOptionalArg()
                    .ofType(String.class);

            this.addOpt = parser.accepts("add", "Used with alter: add the specified resource.");

            this.removeOpt = parser.accepts("remove", "Used with alter: remove the specified resource.");

            this.virtualClusterOpt = parser.accepts("virtual-cluster", "REQUIRED for create, delete, alter or describe: The name of the virtual cluster.")
                    .withRequiredArg()
                    .ofType(String.class);

            this.topicOpt = parser.accepts("topic", "Used with alter: the physical topic name to link.")
                    .withRequiredArg()
                    .ofType(String.class);

            this.userOpt = parser.accepts("user", "Used with alter: the user principal to add or remove.")
                    .withRequiredArg()
                    .ofType(String.class);

            this.clientOpt = parser.accepts("client", "Used with alter: the client-id to add or remove.")
                    .withRequiredArg()
                    .ofType(String.class);

            this.groupOpt = parser.accepts("group", "Used with alter: the consumer group to add or remove.")
                    .withRequiredArg()
                    .ofType(String.class);

            this.transactionalIdOpt = parser.accepts("transactional-id", "Used with alter: the transactional-id to add or remove.")
                    .withRequiredArg()
                    .ofType(String.class);

            this.linkOpt = parser.accepts("link", "Used with alter --topic --add: the link name to assign to the topic.")
                    .withRequiredArg()
                    .ofType(String.class);

            this.actionArg = parser.nonOptions("Action to perform: create, delete, alter, list, describe")
                    .ofType(String.class);

            options = parser.parse(args);
        }

        public String action() {
            List<String> nonOpts = options.valuesOf(actionArg);
            return nonOpts.isEmpty() ? "" : nonOpts.get(0);
        }

        public String virtualClusterName() {
            return options.valueOf(virtualClusterOpt);
        }

        /** Returns the single entity change derived from whichever per-type flag is present. */
        public VirtualClusterAlteration.VirtualClusterEntityChange resolveChange(
                VirtualClusterAlteration.ResourceChangeType changeType) {
            if (options.has(topicOpt)) {
                String physicalName = options.valueOf(topicOpt);
                String link = options.has(linkOpt) ? options.valueOf(linkOpt) : null;
                return new VirtualClusterAlteration.VirtualClusterEntityChange(
                        VirtualClusterAlteration.ResourceType.TOPIC, physicalName, link, changeType);
            } else if (options.has(userOpt)) {
                return new VirtualClusterAlteration.VirtualClusterEntityChange(
                        VirtualClusterAlteration.ResourceType.USER, options.valueOf(userOpt), changeType);
            } else if (options.has(clientOpt)) {
                return new VirtualClusterAlteration.VirtualClusterEntityChange(
                        VirtualClusterAlteration.ResourceType.CLIENT, options.valueOf(clientOpt), changeType);
            } else if (options.has(groupOpt)) {
                return new VirtualClusterAlteration.VirtualClusterEntityChange(
                        VirtualClusterAlteration.ResourceType.GROUP, options.valueOf(groupOpt), changeType);
            } else {
                return new VirtualClusterAlteration.VirtualClusterEntityChange(
                        VirtualClusterAlteration.ResourceType.TRANSACTIONAL_ID,
                        options.valueOf(transactionalIdOpt), changeType);
            }
        }

        public boolean isAddOpt() {
            return options.has(addOpt);
        }

        public void checkArgs() {
            CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt);

            switch (action()) {
                case "create":
                    CommandLineUtils.checkRequiredArgs(parser, options, virtualClusterOpt);
                    break;
                case "delete":
                    CommandLineUtils.checkRequiredArgs(parser, options, virtualClusterOpt);
                    break;
                case "alter":
                    CommandLineUtils.checkRequiredArgs(parser, options, virtualClusterOpt);
                    if (!options.has(addOpt) && !options.has(removeOpt)) {
                        CommandLineUtils.printUsageAndExit(parser, "alter requires either --add or --remove");
                    }
                    if (options.has(addOpt) && options.has(removeOpt)) {
                        CommandLineUtils.printUsageAndExit(parser, "--add and --remove are mutually exclusive");
                    }
                    // Exactly one resource-type flag must be present
                    int resourceFlagCount = 0;
                    if (options.has(topicOpt))          resourceFlagCount++;
                    if (options.has(userOpt))           resourceFlagCount++;
                    if (options.has(clientOpt))         resourceFlagCount++;
                    if (options.has(groupOpt))          resourceFlagCount++;
                    if (options.has(transactionalIdOpt)) resourceFlagCount++;
                    if (resourceFlagCount == 0) {
                        CommandLineUtils.printUsageAndExit(parser,
                            "alter requires exactly one of: --topic, --user, --client, --group, --transactional-id");
                    }
                    if (resourceFlagCount > 1) {
                        CommandLineUtils.printUsageAndExit(parser,
                            "--topic, --user, --client, --group and --transactional-id are mutually exclusive");
                    }
                    // --link is required when adding a topic, forbidden otherwise
                    if (options.has(topicOpt) && options.has(addOpt) && !options.has(linkOpt)) {
                        CommandLineUtils.printUsageAndExit(parser, "--topic --add requires --link <link-name>");
                    }
                    if (!options.has(topicOpt) && options.has(linkOpt)) {
                        CommandLineUtils.printUsageAndExit(parser, "--link is only valid with --topic");
                    }
                    break;
                case "list":
                    // no additional args required
                    break;
                case "describe":
                    CommandLineUtils.checkRequiredArgs(parser, options, virtualClusterOpt);
                    break;
                default:
                    CommandLineUtils.printUsageAndExit(parser, "Command must include exactly one action: create, delete, alter, list or describe");
            }
        }
    }
}
