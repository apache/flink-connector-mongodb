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
                        .withCreateContainerCmdModifier(it -> it.withHostName(CONFIG_HOSTNAME))
                        .withCommand(
                                "-configsvr",
                                "--replSet",
                                CONFIG_REPLICA_SET_NAME,
                                "--port",
                                String.valueOf(MONGODB_INTERNAL_PORT))
                        .withNetwork(network)
                        .withLogConsumer(logConsumer);
        this.shardSrv =
                new MongoDBContainer(dockerImageName)
                        .withCreateContainerCmdModifier(it -> it.withHostName(SHARD_HOSTNAME))
                        .withCommand(
                                "-shardsvr",
                                "--replSet",
                                SHARD_REPLICA_SET_NAME,
                                "--port",
                                String.valueOf(MONGODB_INTERNAL_PORT))
                        .withNetwork(network)
                        .withLogConsumer(logConsumer);
        this.router =
                new MongoRouterContainer(dockerImageName)
                        .withCreateContainerCmdModifier(it -> it.withHostName(ROUTER_HOSTNAME))
                        .withNetwork(network)
                        .dependsOn(configSrv, shardSrv)
                        .withLogConsumer(logConsumer);
    }

    public void start() {
        LOG.info("Starting ConfigSrv container");
        configSrv.start();
        LOG.info("Starting ShardSrv container");
        shardSrv.start();
        LOG.info("Starting Router containers");
        router.start();
    }

    public void close() {
        router.stop();
        shardSrv.stop();
        configSrv.stop();
    }

    @Override
    public void beforeAll(ExtensionContext context) {
        start();
    }

    @Override
    public void afterAll(ExtensionContext context) {
        close();
    }

    public String getConnectionString() {
        return String.format(
                "mongodb://%s:%d", router.getHost(), router.getMappedPort(MONGODB_INTERNAL_PORT));
    }

    private static class MongoRouterContainer extends MongoDBContainer {
        private MongoRouterContainer(DockerImageName dockerImageName) {
            super(dockerImageName);
            withCommand(
                    "mongos",
                    "--bind_ip_all",
                    "--configdb",
                    String.format(
                            "%s/%s:%d",
                            CONFIG_REPLICA_SET_NAME, CONFIG_HOSTNAME, MONGODB_INTERNAL_PORT));
        }

        @Override
        protected void containerIsStarted(InspectContainerResponse containerInfo) {
            addShard();
        }

        private void addShard() {
            try {
                String addShardCommand =
                        String.format(
                                "sh.addShard('%s/%s:%d')",
                                SHARD_REPLICA_SET_NAME, SHARD_HOSTNAME, MONGODB_INTERNAL_PORT);
                ExecResult execResult = execInContainer("mongo", "--eval", addShardCommand);
                LOG.info(execResult.getStdout());
                if (execResult.getExitCode() != 0) {
                    throw new IllegalStateException(
                            "Execute mongo command failed " + execResult.getStdout());
                }
            } catch (InterruptedException | IOException e) {
                throw new IllegalStateException("Execute mongo command failed", e);
            }
        }
    }
}
