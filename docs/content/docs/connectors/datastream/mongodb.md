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

# MongoDB Connector
Flink provides a [MongoDB](https://www.mongodb.com/) connector for reading and writing data from
and to MongoDB collections with at-least-once guarantees.

To use this connector, add one of the following dependencies to your project.

{{< connector_artifact flink-connector-mongodb mongodb >}}

## MongoDB Source

The example below shows how to configure and create a source:

{{< tabs "d954f0e8-576a-4152-bfe7-b70bf46c34d2" >}}
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

### Configurations
Flink's MongoDB source is created by using the static builder `MongoSource.<OutputType>builder()`.

1. __setUri(String uri)__
    * Required.
    * Sets the [connection string](https://www.mongodb.com/docs/manual/reference/connection-string/) of MongoDB.
2. __setDatabase(String database)__
    * Required.
    * Name of the database to read from.
3. __setCollection(String collection)__
    * Required.
    * Name of the collection to read from.
4. _setFetchSize(int fetchSize)_
    * Optional. Default: `2048`.
    * Sets the number of documents should be fetched per round-trip when reading.
5. _setNoCursorTimeout(boolean noCursorTimeout)_
    * Optional. Default: `true`.
    * The MongoDB server normally times out idle cursors after an inactivity period (10 minutes) to
      prevent excess memory use. Set this option to prevent that. If a session is idle for longer
      than 30 minutes, the MongoDB server marks that session as expired and may close it at any
      time. When the MongoDB server closes the session, it also kills any in-progress operations
      and open cursors associated with the session. This includes cursors configured with
      `noCursorTimeout()` or a `maxTimeMS()` greater than 30 minutes.
6. _setPartitionStrategy(PartitionStrategy partitionStrategy)_
    * Optional. Default: `PartitionStrategy.DEFAULT`.
    * Sets the partition strategy. Available partition strategies are `SINGLE`, `SAMPLE`, `SPLIT_VECTOR`,
      `SHARDED` and `DEFAULT`. You can see <a href="#partition-strategies">Partition Strategies section</a> for detail.
7. _setPartitionSize(MemorySize partitionSize)_
    * Optional. Default: `64mb`.
    * Sets the partition memory size of MongoDB split. Split a MongoDB collection into multiple
      partitions according to the partition memory size. Partitions can be read in parallel by
      multiple readers to speed up the overall read time.
8. _setSamplesPerPartition(int samplesPerPartition)_
    * Optional. Default: `10`.
    * Sets the number of samples to take per partition which is only used for the sample partition
      strategy `SAMPLE`. The sample partitioner samples the collection, projects and sorts by the
      partition fields. Then uses every `samplesPerPartition` as the value to use to calculate the
      partition boundaries. The total number of samples taken is:
      `samples per partition * ( count of documents / number of documents per partition)`.
9. _setLimit(int limit)_
    * Optional. Default: `-1`.
    * Sets the limit of documents for each reader to read. If limit is not set or set to -1, 
      the documents of the entire collection will be read. If we set the parallelism of reading to
      be greater than 1, the maximum documents to read is equal to the `parallelism * limit`.
10. _setProjectedFields(String... projectedFields)_
    * Optional.
    * Sets the projection fields of documents to read. If projected fields is not set, all fields of
      the collection will be read.
11. __setDeserializationSchema(MongoDeserializationSchema<OutputType> deserializationSchema)__
    * Required.
    * A `MongoDeserializationSchema` is required for parsing MongoDB BSON documents.

### Partition Strategies
Partitions can be read in parallel by multiple readers to speed up the overall read time.
The following partition strategies are provided:

- `SINGLE`: treats the entire collection as a single partition.
- `SAMPLE`: samples the collection and generate partitions which is fast but possibly uneven.
- `SPLIT_VECTOR`: uses the splitVector command to generate partitions for non-sharded
  collections which is fast and even. The splitVector permission is required.
- `SHARDED`: reads `config.chunks` (MongoDB splits a sharded collection into chunks, and the
  range of the chunks are stored within the collection) as the partitions directly. The
  sharded strategy only used for sharded collection which is fast and even. Read permission
  of config database is required.
- `DEFAULT`: uses sharded strategy for sharded collections otherwise using split vector
  strategy.

## MongoDB Sink
The example below shows how to configure and create a sink:

{{< tabs "4a96142b-57e4-490f-897c-59ed12c2caef" >}}
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

### Configurations
Flink's MongoDB sink is created by using the static builder `MongoSink.<InputType>builder()`.

1. __setUri(String uri)__
    * Required.
    * Sets the [connection string](https://www.mongodb.com/docs/manual/reference/connection-string/) of MongoDB.
2. __setDatabase(String database)__
    * Required.
    * Name of the database to sink to.
3. __setCollection(String collection)__
    * Required.
    * Name of the collection to sink to.
4. _setBatchSize(int batchSize)_
    * Optional. Default: `1000`.
    * Sets the maximum number of actions to buffer for each batch request.
      You can pass -1 to disable batching.
5. _setBatchIntervalMs(long batchIntervalMs)_
    * Optional. Default: `1000`.
    * Sets the batch flush interval, in milliseconds. You can pass -1 to disable it.
6. _setMaxRetries(int maxRetries)_
    * Optional. Default: `3`.
    * Sets the max retry times if writing records failed.
7. _setDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee)_
    * Optional. Default: `DeliveryGuarantee.AT_LEAST_ONCE`.
    * Sets the wanted `DeliveryGuarantee`. The `EXACTLY_ONCE` guarantee is not supported yet.
8. _setOrderedWrites(boolean ordered)_
    * Optional. Default: `true`
    * Defines MongoDB driver option to perform ordered writes.
8. _setBypassDocumentValidation(boolean bypassDocumentValidation)_
    * Optional. Default: `false`
    * Defines MongoDB driver option to bypass document validation. 
9. __setSerializationSchema(MongoSerializationSchema<InputType> serializationSchema)__
    * Required.
    * A `MongoSerializationSchema` is required for parsing input record to MongoDB 
      [WriteModel](https://www.mongodb.com/docs/drivers/java/sync/current/usage-examples/bulkWrite/).

### Fault Tolerance

With Flink’s checkpointing enabled, the Flink MongoDB Sink guarantees
at-least-once delivery of write operations to MongoDB clusters. It does
so by waiting for all pending write operations in the `MongoWriter` at the
time of checkpoints. This effectively assures that all requests before the
checkpoint was triggered have been successfully acknowledged by MongoDB, before
proceeding to process more records sent to the sink.

More details on checkpoints and fault tolerance are in the [fault tolerance docs]({{< ref "docs/learn-flink/fault_tolerance" >}}).

To use fault tolerant MongoDB Sinks, checkpointing of the topology needs to be enabled at the execution environment:

{{< tabs "aae2c01d-8f80-4f5f-a707-333607f35669" >}}
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
<b>IMPORTANT</b>: Checkpointing is not enabled by default but the default delivery guarantee is AT_LEAST_ONCE.
This causes the sink to buffer requests until it either finishes or the `MongoWriter` flushes automatically. 
By default, the `MongoWriter` will flush after 1000 added write operations. To configure the writer to flush more frequently,
please refer to the <a href="#configuring-the-internal-mongo-writer">MongoWriter configuration section</a>.
</p>

<p style="border-radius: 5px; padding: 5px" class="bg-info">
Using WriteModel with deterministic ids and the upsert method it is possible to achieve exactly-once 
semantics in MongoDB when AT_LEAST_ONCE delivery is configured for the connector.
</p>

### Configuring the Internal Mongo Writer

The internal `MongoWriter` can be further configured for its behaviour on how write operations are 
flushed, by using the following methods of the `MongoSinkBuilder`:

- **setBatchSize(int batchSize)**: Maximum amount of write operations to buffer before flushing. You can pass -1 to disable it.
- **setBatchIntervalMs(long batchIntervalMs)**: Interval at which to flush regardless of the size of buffered write operations. You can pass -1 to disable it.

When set as follows, there are the following writing behaviours:

- Flush when time interval or batch size exceed limit.
   * batchSize > 1 and batchInterval > 0
- Flush only on checkpoint.
   * batchSize == -1 and batchInterval == -1 
- Flush for every single write operation.
   * batchSize == 1 or batchInterval == 0
