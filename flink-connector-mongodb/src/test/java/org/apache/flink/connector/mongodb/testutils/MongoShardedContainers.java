/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *   http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.connector.mongodb.testutils;

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;

/** Sharded Containers. */
public class MongoShardedContainers implements BeforeAllCallback, AfterAllCallback {

    private static final Logger LOG = LoggerFactory.getLogger(MongoShardedContainers.class);

    private static final int MONGODB_INTERNAL_PORT = 27017;

    private static final String CONFIG_REPLICA_SET_NAME = "rs-config-0";
    private static final String SHARD_REPLICA_SET_NAME = "rs-shard-0";

    private static final String CONFIG_HOSTNAME = "config-0";
    private static final String SHARD_HOSTNAME = "shard-0";
    private static final String ROUTER_HOSTNAME = "router-0";

    private final MongoDBContainer configSrv;
    private final MongoDBContainer shardSrv;
    private final MongoDBContainer router;

    MongoShardedContainers(DockerImageName dockerImageName, Network network) {
        Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(LOG);
        this.configSrv =
                new MongoDBContainer(dockerImageName)
                        .withCreateContainerCmdModifier(
                                it ->
                                        it.withCmd(
                                                        "-configsvr",
                                                        "--replSet",
                                                        CONFIG_REPLICA_SET_NAME,
                                                        "--port",
                                                        String.valueOf(MONGODB_INTERNAL_PORT))
                                                .withHostName(CONFIG_HOSTNAME))
                        .withNetwork(network)
                        .withNetworkAliases(CONFIG_HOSTNAME)
                        .withLogConsumer(logConsumer);
        this.shardSrv =
                new MongoDBContainer(dockerImageName)
                        .withCreateContainerCmdModifier(
                                it ->
                                        it.withCmd(
                                                        "-shardsvr",
                                                        "--replSet",
                                                        SHARD_REPLICA_SET_NAME,
                                                        "--port",
                                                        String.valueOf(MONGODB_INTERNAL_PORT))
                                                .withHostName(SHARD_HOSTNAME))
                        .withNetwork(network)
                        .withNetworkAliases(SHARD_HOSTNAME)
                        .withLogConsumer(logConsumer);
        this.router =
                new MongoRouterContainer(dockerImageName)
                        .withCreateContainerCmdModifier(it -> it.withHostName(ROUTER_HOSTNAME))
                        .dependsOn(configSrv, shardSrv)
                        .withNetwork(network)
                        .withNetworkAliases(ROUTER_HOSTNAME)
                        .withLogConsumer(logConsumer);
    }

    @Override
    public void beforeAll(ExtensionContext context) {
        LOG.info("Starting ConfigSrv container");
        configSrv.start();
        LOG.info("Starting ShardSrv container");
        shardSrv.start();
        LOG.info("Starting Router containers");
        router.start();
    }

    @Override
    public void afterAll(ExtensionContext context) {
        router.stop();
        shardSrv.stop();
        configSrv.stop();
    }

    public String getConnectionString() {
        return String.format(
                "mongodb://%s:%d", router.getHost(), router.getMappedPort(MONGODB_INTERNAL_PORT));
    }

    public void executeCommand(String command) {
        executeCommand(router, command);
    }

    private static void executeCommand(MongoDBContainer container, String command) {
        try {
            ExecResult execResult = container.execInContainer(buildMongoEvalCommand(command));
            LOG.info(execResult.getStdout());
            if (execResult.getExitCode() != 0) {
                throw new IllegalStateException(
                        "Execute mongo command failed " + execResult.getStderr());
            }
        } catch (InterruptedException | IOException e) {
            throw new IllegalStateException("Execute mongo command failed", e);
        }
    }

    private static String[] buildMongoEvalCommand(final String command) {
        return new String[] {
            "sh",
            "-c",
            "mongosh mongo --eval \"" + command + "\"  || mongo --eval \"" + command + "\"",
        };
    }

    private static class MongoRouterContainer extends MongoDBContainer {

        private MongoRouterContainer(DockerImageName dockerImageName) {
            super(dockerImageName);
            withCreateContainerCmdModifier(
                    it ->
                            it.withCmd(
                                    "mongos",
                                    "--bind_ip_all",
                                    "--configdb",
                                    String.format(
                                            "%s/%s:%d",
                                            CONFIG_REPLICA_SET_NAME,
                                            CONFIG_HOSTNAME,
                                            MONGODB_INTERNAL_PORT)));
        }

        @Override
        protected void containerIsStarted(InspectContainerResponse containerInfo, boolean reused) {
            addShard();
        }

        private void addShard() {
            String addShardCommand =
                    String.format(
                            "sh.addShard('%s/%s:%d');",
                            SHARD_REPLICA_SET_NAME, SHARD_HOSTNAME, MONGODB_INTERNAL_PORT);
            executeCommand(this, addShardCommand);

            String addShardToZoneCommand =
                    "sh.addShardToZone('${shard}', 'zone-0');"
                            + "sh.addShardToZone('${shard}', 'zone-1');";
            executeCommand(this, addShardToZoneCommand.replace("${shard}", SHARD_REPLICA_SET_NAME));
        }
    }
}
