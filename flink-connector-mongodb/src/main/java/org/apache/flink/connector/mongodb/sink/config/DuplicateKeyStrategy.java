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

package org.apache.flink.connector.mongodb.sink.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.description.InlineElement;

import static org.apache.flink.configuration.description.TextElement.text;

/**
 * Strategy for handling duplicate key errors (E11000) during MongoDB bulk writes.
 *
 * <p>Duplicate key errors occur when inserting records that violate a unique index constraint. This
 * is common during AT_LEAST_ONCE replay after Flink task recovery, where records from the last
 * checkpoint may be replayed and already exist in MongoDB.
 */
@PublicEvolving
public enum DuplicateKeyStrategy implements DescribedEnum {
    FAIL(
            "fail",
            text("Retry the batch unchanged up to maxRetries, then fail with an IOException.")),

    SKIP_DUPLICATES(
            "skip-duplicates",
            text(
                    "Skip records that cause duplicate key errors (E11000). "
                            + "For ordered writes, re-queue remaining operations after the error index. "
                            + "For unordered writes, consider non-duplicate operations as succeeded."));

    private final String name;
    private final InlineElement description;

    DuplicateKeyStrategy(String name, InlineElement description) {
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
