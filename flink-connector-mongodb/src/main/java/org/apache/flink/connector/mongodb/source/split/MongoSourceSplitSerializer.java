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

package org.apache.flink.connector.mongodb.source.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.bson.BsonDocument;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

import static org.apache.flink.connector.mongodb.common.utils.MongoSerdeUtils.deserializeMap;
import static org.apache.flink.connector.mongodb.common.utils.MongoSerdeUtils.serializeMap;

/** The {@link SimpleVersionedSerializer serializer} for {@link MongoSourceSplit}. */
@Internal
public class MongoSourceSplitSerializer implements SimpleVersionedSerializer<MongoSourceSplit> {

    public static final MongoSourceSplitSerializer INSTANCE = new MongoSourceSplitSerializer();

    // This version should be bumped after modifying the MongoSourceSplit.
    public static final int CURRENT_VERSION = 1;

    public static final int SCAN_SPLIT_FLAG = 1;
    public static final int STREAM_SPLIT_FLAG = 2;

    private MongoSourceSplitSerializer() {}

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(MongoSourceSplit obj) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            serializeMongoSplit(out, obj);
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public MongoSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        // VERSION 0 deserialization
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            int splitKind = in.readInt();
            if (splitKind == SCAN_SPLIT_FLAG) {
                return deserializeMongoScanSourceSplit(version, in);
            }
            if (splitKind == STREAM_SPLIT_FLAG) {
                return deserializeMongoStreamSourceSplit(version, in);
            }
            throw new IOException("Unknown split kind: " + splitKind);
        }
    }

    public void serializeMongoSplit(DataOutputStream out, MongoSourceSplit obj) throws IOException {
        if (obj instanceof MongoScanSourceSplit) {
            MongoScanSourceSplit split = (MongoScanSourceSplit) obj;
            out.writeInt(SCAN_SPLIT_FLAG);
            out.writeUTF(split.splitId());
            out.writeUTF(split.getDatabase());
            out.writeUTF(split.getCollection());
            out.writeUTF(split.getMin().toJson());
            out.writeUTF(split.getMax().toJson());
            out.writeUTF(split.getHint().toJson());
            out.writeInt(split.getOffset());
        } else if (obj instanceof MongoStreamSourceSplit) {
            MongoStreamSourceSplit split = (MongoStreamSourceSplit) obj;
            out.writeInt(STREAM_SPLIT_FLAG);
            out.writeUTF(split.splitId());
            out.writeUTF(split.getDatabase());
            out.writeUTF(split.getCollection());
            serializeMap(
                    out,
                    split.streamOffset().getOffset(),
                    DataOutputStream::writeUTF,
                    DataOutputStream::writeUTF);
        } else {
            throw new IOException("Unknown split kind: " + obj.getClass().getName());
        }
    }

    public MongoScanSourceSplit deserializeMongoScanSourceSplit(int version, DataInputStream in)
            throws IOException {
        switch (version) {
            case 0:
            case 1:
                String splitId = in.readUTF();
                String database = in.readUTF();
                String collection = in.readUTF();
                BsonDocument min = BsonDocument.parse(in.readUTF());
                BsonDocument max = BsonDocument.parse(in.readUTF());
                BsonDocument hint = BsonDocument.parse(in.readUTF());
                int offset = in.readInt();
                return new MongoScanSourceSplit(
                        splitId, database, collection, min, max, hint, offset);
            default:
                throw new IOException("Unknown version: " + version);
        }
    }

    public MongoStreamSourceSplit deserializeMongoStreamSourceSplit(int version, DataInputStream in)
            throws IOException {
        switch (version) {
            case 0:
            case 1:
                String splitId = in.readUTF();
                String database = in.readUTF();
                String collection = in.readUTF();

                Map<String, String> offset =
                        deserializeMap(in, DataInput::readUTF, DataInput::readUTF);
                MongoStreamOffset streamOffset = MongoStreamOffset.fromOffset(offset);
                return new MongoStreamSourceSplit(splitId, database, collection, streamOffset);
            default:
                throw new IOException("Unknown version: " + version);
        }
    }
}
