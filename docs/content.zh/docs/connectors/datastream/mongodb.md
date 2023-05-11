---
title: MongoDB
weight: 5
type: docs
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# MongoDB 连接器
Flink 提供了 [MongoDB](https://www.mongodb.com/) 连接器使用至少一次（At-least-once）的语义在 MongoDB collection 中读取和写入数据。

要使用此连接器，请将以下依赖添加到你的项目中：

{{< connector_artifact flink-connector-mongodb mongodb >}}

## MongoDB Source
下面的例子介绍了如何构建一个 MongoDB Source：

{{< tabs "a5da31f0-56ed-4c8a-87f6-d02c7867f795" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.mongodb.source.MongoSource;
import org.apache.flink.connector.mongodb.source.reader.deserializer.MongoDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.bson.BsonDocument;

MongoSource<String> source = MongoSource.<String>builder()
        .setUri("mongodb://user:password@127.0.0.1:27017")
        .setDatabase("my_db")
        .setCollection("my_coll")
        .setProjectedFields("_id", "f0", "f1")
        .setFetchSize(2048)
        .setLimit(10000)
        .setNoCursorTimeout(true)
        .setPartitionStrategy(PartitionStrategy.SAMPLE)
        .setPartitionSize(MemorySize.ofMebiBytes(64))
        .setSamplesPerPartition(10)
        .setDeserializationSchema(new MongoDeserializationSchema<String>() {
            @Override
            public String deserialize(BsonDocument document) {
                return document.toJson();
            }

            @Override
            public TypeInformation<String> getProducedType() {
                return BasicTypeInfo.STRING_TYPE_INFO;
            }
        })
        .build();

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

env.fromSource(source, WatermarkStrategy.noWatermarks(), "MongoDB-Source")
        .setParallelism(2)
        .print()
        .setParallelism(1);
```
{{< /tab >}}
{{< /tabs >}}

### 配置项
Flink 的 MongoDB Source 可以通过 `MongoSource.<OutputType>builder()` 构造器构建。

1. __setUri(String uri)__
    * 必须。
    * 设置 MongoDB [连接字符串](https://www.mongodb.com/docs/manual/reference/connection-string/)。
2. __setDatabase(String database)__
    * 必须。
    * 设置读取的数据库名称。
3. __setCollection(String collection)__
    * 必须。
    * 设置读取的集合名称。
4. _setFetchSize(int fetchSize)_
    * 可选。默认值： `2048`。
    * 设置每次循环读取时应该从游标中获取的行数。
5. _setNoCursorTimeout(boolean noCursorTimeout)_
    * 可选。默认值： `true`。
    * MongoDB 服务端通常会将空闲时间超过 10 分钟的 cursor 关闭，来节省内存开销。将这个参数设置为 true 可以
      防止 cursor 因为读取时间过长或者背压导致的空闲而关闭。需要注意的是。如果 cursor 所在的会话空闲时间超过
      了 30 分钟，MongoDB 会关闭当前会话以及由当前会话打开的 cursors，不管有没有设置 `noCursorTimeout()` 
      或者 `maxTimeMS()` 大于 30分钟。
6. _setPartitionStrategy(PartitionStrategy partitionStrategy) _
    * 可选。 默认值： `PartitionStrategy.DEFAULT`。
    * 设置分区策略。 可选的分区策略有 `SINGLE`，`SAMPLE`，`SPLIT_VECTOR`，`SHARDED` 和 `DEFAULT`。
      查看 <a href="#分区策略">分区策略</a> 章节获取详细介绍。
7. _setPartitionSize(MemorySize partitionSize)_
    * 可选。默认值：`64mb`。
    * 设置每个分区的内存大小。通过指定的分区大小，将 MongoDB 的一个集合切分成多个分区。
      可以设置并行度，并行地读取这些分区，以提升整体的读取速度。
8. _setSamplesPerPartition(int samplesPerPartition)_
    * 可选。默认值：`10`。
    * 仅用于 `SAMPLE` 抽样分区策略，设置每个分区的样本数量。抽样分区器根据分区键对集合进行随机采样的方式计算分区边界。
      `总的样本数量 = 每个分区的样本数量 * (文档总数 / 每个分区的文档数量)`
9. _setLimit(int limit)_
    * 可选。默认值：`-1`。
    * 限制每个 reader 最多读取文档的数量。如果我们设置了读取并行度大于 1，那么最多读取的文档数量等于 `并行度 * 限制数量`。
10. _setProjectedFields(String... projectedFields)_
    * 可选。
    * 设置读取文档的部分字段。如果没有设置，会读取文档的全部字段。
11. __setDeserializationSchema(MongoDeserializationSchema<OutputType> deserializationSchema)__
    * 必须。
    * 设置 `MongoDeserializationSchema` 用于解析 MongoDB BSON 类型的文档。

### 分区策略
使用分区可以利用并行读取来加速整体的读取效率。目前提供了以下几种分区策略：

- `SINGLE`：将整个集合作为一个分区。
- `SAMPLE`：通过随机采样的方式来生成分区，快速但可能不均匀。
- `SPLIT_VECTOR`：通过 MongoDB 计算分片的 splitVector 命令来生成分区，快速且均匀。
  仅适用于未分片集合，需要 splitVector 权限。
- `SHARDED`：从 `config.chunks` 集合中直接读取分片集合的分片边界作为分区，不需要额外计算，快速且均匀。
  仅适用于已经分片的集合，需要 config 数据库的读取权限。
- `DEFAULT`：对分片集合使用 `SHARDED` 策略，对未分片集合使用 `SPLIT_VECTOR` 策略。

## MongoDB Sink
下面的例子介绍了如何构建一个 MongoDB Sink：

{{< tabs "e72bd5ec-6c23-4bc1-85e7-c42290cffa77" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.apache.flink.streaming.api.datastream.DataStream;

import com.mongodb.client.model.InsertOneModel;
import org.bson.BsonDocument;

DataStream<String> stream = ...;

MongoSink<String> sink = MongoSink.<String>builder()
        .setUri("mongodb://user:password@127.0.0.1:27017")
        .setDatabase("my_db")
        .setCollection("my_coll")
        .setBatchSize(1000)
        .setBatchIntervalMs(1000)
        .setMaxRetries(3)
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .setSerializationSchema(
                (input, context) -> new InsertOneModel<>(BsonDocument.parse(input)))
        .build();

stream.sinkTo(sink);
```
{{< /tab >}}
{{< /tabs >}}

### 配置项
Flink 的 MongoDB Source 可以通过 `MongoSink.<InputType>builder()` 构造器构建。

1. __setUri(String uri)__
    * 必须。
    * 设置 MongoDB [连接字符串](https://www.mongodb.com/docs/manual/reference/connection-string/)。
2. __setDatabase(String database)__
    * 必须。
    * 设置写入的数据库名称。
3. __setCollection(String collection)__
    * 必须。
    * 设置写入的集合名称。
4. _setBatchSize(int batchSize)_
    * 可选。默认值：`1000`。
    * 设置写入的最大批次大小。可以设置为 -1 来禁用批式写入。
5. _setBatchIntervalMs(long batchIntervalMs)_
    * 可选。默认值：`1000`.
    * 设置写入的最大间隔时间，单位为毫秒。可以设置为 -1 来禁用批式写入。
6. _setMaxRetries(int maxRetries)_
    * 可选。默认值：`3`。
    * 设置写入失败时最大重试次数。
7. _setDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee)_
    * 可选。默认值：`DeliveryGuarantee.AT_LEAST_ONCE`.
    * 设置投递保证。 仅一次（EXACTLY_ONCE）的投递保证暂不支持。
8. __setSerializationSchema(MongoSerializationSchema<InputType> serializationSchema)__
    * 必须。
    * 设置 `MongoSerializationSchema` 将输入类型转换为 MongoDB
      [WriteModel](https://www.mongodb.com/docs/drivers/java/sync/current/usage-examples/bulkWrite/)。

### 容错
当开启了 Flink checkpoint，Flink MongoDB Sink 保证至少一次 (at-least-once) 的写入语义.
`MongoWriter` 在检查点时，会确认所有被缓存的写入操作被正确写入.

关于检查点和容错相关的更多信息，请参阅 [容错文档]({{< ref "docs/learn-flink/fault_tolerance" >}}).

下面的示例介绍了如何开启检查点：

{{< tabs "e13337f3-8e88-4368-af41-850c9fa20cd9" >}}
{{< tab "Java" >}}
```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(5000); // checkpoint every 5000 msecs
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment()
env.enableCheckpointing(5000) // checkpoint every 5000 msecs
```

{{< /tab >}}
{{< tab "Python" >}}

```python
env = StreamExecutionEnvironment.get_execution_environment()
# checkpoint every 5000 msecs
env.enable_checkpointing(5000)
```

{{< /tab >}}
{{< /tabs >}}

<p style="border-radius: 5px; padding: 5px" class="bg-info">
<b>重要提醒</b>: 尽管默认的写入语义为至少一次 (AT_LEAST_ONCE)，但检查点并不是默认开启的.
这会导致 Sink 缓冲写入请求，直到完成或 `MongoWriter` 自动刷新.
默认情况下，`MongoWriter` 会缓冲 1000 个新增的写入请求。需要使写入更加频繁，请参考 <a href="#配置-Mongo-Writer">配置 MongoWriter</a>。
</p>

<p style="border-radius: 5px; padding: 5px" class="bg-info">
当开启 AT_LEAST_ONCE 投递时，通过指定明确的主键和 upsert 方式写入，可以实现仅一次 (exactly-once) 的写入语义。
</p>

### 配置 Mongo Writer

内部的 `MongoWriter` 可以使用 `MongoSinkBuilder` 更精细化地配置来实现不一样的写入行为：

- **setBatchSize(int batchSize)**：设置写入的批次大小。 可以设置为 -1 来禁用批式写入。
- **setBatchIntervalMs(long batchIntervalMs)**：设置写入的最大间隔时间，单位为毫秒。 可以设置为 -1 来禁用批式写入。

当使用如下设置时，会产生不一样的写入行为：

- 当缓存记录数超过最大批次大小，或者写入时间间隔超过限制时写入。
   * batchSize > 1 and batchInterval > 0
- 仅在检查点写入。
   * batchSize == -1 and batchInterval == -1 
- 对每条记录进行写入。
   * batchSize == 1 or batchInterval == 0
