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

package org.apache.flink.connector.mongodb.source.reader.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.mongodb.source.reader.MongoSourceRecord;
import org.apache.flink.connector.mongodb.source.split.MongoSourceSplit;

/** A split reader implements {@link SplitReader} for {@link MongoSourceSplit}. */
@Internal
public interface MongoSourceSplitReader extends SplitReader<MongoSourceRecord, MongoSourceSplit> {
    /** Whether the current reader can accept the next split. */
    boolean canAcceptNextSplit();
}
