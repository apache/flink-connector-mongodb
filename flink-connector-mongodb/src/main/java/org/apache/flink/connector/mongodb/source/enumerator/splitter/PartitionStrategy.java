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

package org.apache.flink.connector.mongodb.source.enumerator.splitter;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.description.InlineElement;

import static org.apache.flink.configuration.description.TextElement.text;

/**
 * Partition strategies that can be chosen. Available strategies are single, sample, split-vector,
 * sharded and default.
 *
 * <ul>
 *   <li>single: treats the entire collection as a single partition.
 *   <li>sample: samples the collection and generate partitions which is fast but possibly uneven.
 *   <li>split-vector: uses the splitVector command to generate partitions for non-sharded
 *       collections which is fast and even. The splitVector permission is required.
 *   <li>sharded: reads config.chunks (MongoDB splits a sharded collection into chunks, and the
 *       range of the chunks are stored within the collection) as the partitions directly. The
 *       sharded strategy only used for sharded collection which is fast and even. Read permission
 *       of config database is required.
 *   <li>default: uses sharded strategy for sharded collections otherwise using split vector
 *       strategy.
 * </ul>
 */
@PublicEvolving
public enum PartitionStrategy implements DescribedEnum {
    SINGLE("single", text("Do not split, treat a collection as a single chunk.")),

    SAMPLE("sample", text("Randomly sample the collection, then splits to multiple chunks.")),

    SPLIT_VECTOR(
            "split-vector",
            text("Uses the SplitVector command to generate chunks for non-sharded collections.")),

    SHARDED(
            "sharded",
            text(
                    "Read the chunk ranges from config.chunks collection and splits to multiple chunks. Only support sharded collections.")),

    PAGINATION(
            "pagination",
            text(
                    "Creating chunk records evenly by count. Each chunk will have exactly the same number of records.")),

    DEFAULT(
            "default",
            text(
                    "Using sharded strategy for sharded collections"
                            + " otherwise using split vector strategy."));

    private final String name;
    private final InlineElement description;

    PartitionStrategy(String name, InlineElement description) {
        this.name = name;
        this.description = description;
    }

    @Override
    public InlineElement getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return name;
    }
}
