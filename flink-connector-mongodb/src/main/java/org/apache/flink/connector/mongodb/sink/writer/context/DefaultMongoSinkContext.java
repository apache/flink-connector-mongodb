/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.mongodb.sink.writer.context;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.mongodb.sink.config.MongoWriteOptions;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

import java.util.OptionalLong;

/** Default {@link MongoSinkContext} implementation. */
@Internal
public class DefaultMongoSinkContext implements MongoSinkContext {

    private final Sink.InitContext initContext;
    private final MongoWriteOptions writeOptions;

    public DefaultMongoSinkContext(Sink.InitContext initContext, MongoWriteOptions writeOptions) {
        this.initContext = initContext;
        this.writeOptions = writeOptions;
    }

    @Override
    public long processTime() {
        return initContext.getProcessingTimeService().getCurrentProcessingTime();
    }

    @Override
    public MongoWriteOptions getWriteOptions() {
        return writeOptions;
    }

    @Override
    public UserCodeClassLoader getUserCodeClassLoader() {
        return initContext.getUserCodeClassLoader();
    }

    @Override
    public MailboxExecutor getMailboxExecutor() {
        return initContext.getMailboxExecutor();
    }

    @Override
    public ProcessingTimeService getProcessingTimeService() {
        return initContext.getProcessingTimeService();
    }

    @Override
    public int getSubtaskId() {
        return initContext.getSubtaskId();
    }

    @Override
    public int getNumberOfParallelSubtasks() {
        return initContext.getNumberOfParallelSubtasks();
    }

    @Override
    public SinkWriterMetricGroup metricGroup() {
        return initContext.metricGroup();
    }

    @Override
    public OptionalLong getRestoredCheckpointId() {
        return initContext.getRestoredCheckpointId();
    }

    @Override
    public SerializationSchema.InitializationContext asSerializationSchemaInitializationContext() {
        return initContext.asSerializationSchemaInitializationContext();
    }
}
