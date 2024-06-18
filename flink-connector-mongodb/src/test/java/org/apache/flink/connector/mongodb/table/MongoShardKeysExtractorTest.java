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

package org.apache.flink.connector.mongodb.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MongoShardKeysExtractor}. */
class MongoShardKeysExtractorTest {

    @Test
    void testSingleShardKey() {
        ResolvedSchema schema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("a", DataTypes.BIGINT().notNull()),
                                Column.physical("b", DataTypes.STRING())),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("pk", Collections.singletonList("a")));

        String[] shardKeys = new String[] {"b"};

        Function<RowData, BsonDocument> shardKeysExtractor =
                MongoShardKeysExtractor.createShardKeyExtractor(schema, shardKeys);

        BsonDocument actual =
                shardKeysExtractor.apply(GenericRowData.of(12L, StringData.fromString("ABCD")));
        assertThat(actual).isEqualTo(new BsonDocument("b", new BsonString("ABCD")));
    }

    @Test
    void testCompoundShardKey() {
        ResolvedSchema schema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("a", DataTypes.BIGINT().notNull()),
                                Column.physical("b", DataTypes.STRING().notNull()),
                                Column.physical("c", DataTypes.BIGINT())),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("pk", Collections.singletonList("a")));

        String[] shardKeys = new String[] {"b", "c"};

        Function<RowData, BsonDocument> shardKeysExtractor =
                MongoShardKeysExtractor.createShardKeyExtractor(schema, shardKeys);

        BsonDocument actual =
                shardKeysExtractor.apply(
                        GenericRowData.of(12L, StringData.fromString("ABCD"), 13L));
        assertThat(actual)
                .isEqualTo(
                        new BsonDocument("b", new BsonString("ABCD"))
                                .append("c", new BsonInt64(13L)));
    }

    @Test
    void testCompoundShardKeyWithObjectId() {
        ResolvedSchema schema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("a", DataTypes.STRING().notNull()),
                                Column.physical("b", DataTypes.STRING().notNull()),
                                Column.physical("c", DataTypes.BIGINT())),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("pk", Collections.singletonList("a")));

        String[] shardKeys = new String[] {"a", "b"};

        Function<RowData, BsonDocument> shardKeysExtractor =
                MongoShardKeysExtractor.createShardKeyExtractor(schema, shardKeys);

        ObjectId objectId = new ObjectId();
        BsonDocument actual =
                shardKeysExtractor.apply(
                        GenericRowData.of(
                                StringData.fromString(objectId.toString()),
                                StringData.fromString("ABCD"),
                                13L));
        assertThat(actual)
                .isEqualTo(
                        new BsonDocument("a", new BsonObjectId(objectId))
                                .append("b", new BsonString("ABCD")));
    }
}
