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

package org.apache.flink.connector.mongodb.common.utils;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link MongoSerdeUtils}. */
class MongoSerdeUtilsTest {

    @Test
    public void testSerializeList() throws IOException {
        List<String> expected = Arrays.asList("config.collections", "config.chunks");

        byte[] serialized = serializeList(expected);
        List<String> deserialized = deserializeList(serialized);

        assertThat(deserialized).isEqualTo(expected);
    }

    @Test
    public void testSerializeMap() throws IOException {
        Map<String, String> expected = new HashMap<>();
        expected.put("k0", "v0");
        expected.put("k1", "v1");
        expected.put("k2", "v2");

        byte[] serialized = serializeMap(expected);

        Map<String, String> deserialized = deserializeMap(serialized);

        assertThat(deserialized).isEqualTo(expected);
    }

    private static byte[] serializeList(List<String> list) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            MongoSerdeUtils.serializeList(out, list, DataOutputStream::writeUTF);
            return baos.toByteArray();
        }
    }

    private static List<String> deserializeList(byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            return MongoSerdeUtils.deserializeList(in, DataInput::readUTF);
        }
    }

    private static byte[] serializeMap(Map<String, String> map) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            MongoSerdeUtils.serializeMap(
                    out, map, DataOutputStream::writeUTF, DataOutputStream::writeUTF);
            return baos.toByteArray();
        }
    }

    private static Map<String, String> deserializeMap(byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            return MongoSerdeUtils.deserializeMap(in, DataInput::readUTF, DataInput::readUTF);
        }
    }
}
