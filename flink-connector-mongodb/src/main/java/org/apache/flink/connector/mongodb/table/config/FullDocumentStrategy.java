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

package org.apache.flink.connector.mongodb.table.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.description.InlineElement;

import static org.apache.flink.configuration.description.TextElement.text;

/**
 * FullDocument strategies that can be chosen. Available strategies are update-lookup, and
 * pre-and-post-images.
 *
 * <ul>
 *   <li>update-lookup: prior to version 6.0 of MongoDB, the pre images was not stored in the oplog.
 *       To obtain complete row data, we can use updateLookup feature to lookup the latest snapshot
 *       when the oplog record is accessed.
 *   <li>pre-and-post-images: starting in MongoDB 6.0, you can use change stream events to output
 *       the version of a document before and after changes (the document pre- and post-images).
 * </ul>
 */
@PublicEvolving
public enum FullDocumentStrategy implements DescribedEnum {
    UPDATE_LOOKUP(
            "update-lookup",
            text(
                    "Prior to version 6.0 of MongoDB, the pre images was not stored in the oplog. To obtain complete row data, we can use updateLookup feature to lookup the latest snapshot when the oplog record is accessed.")),
    PRE_AND_POST_IMAGES(
            "pre-and-post-images",
            text(
                    "Starting in MongoDB 6.0, you can use change stream events to output the version of a document before and after changes (the document pre- and post-images)."));

    private final String name;
    private final InlineElement description;

    FullDocumentStrategy(String name, InlineElement description) {
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
