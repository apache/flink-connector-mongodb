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

# MongoDB SQL Connector

{{< label "Scan Source: Bounded" >}}
{{< label "Lookup Source: Sync Mode" >}}
{{< label "Sink: Batch" >}}
{{< label "Sink: Streaming Append & Upsert Mode" >}}

The MongoDB connector allows for reading data from and writing data into MongoDB. 
This document describes how to set up the MongoDB connector to run SQL queries against MongoDB.

The connector can operate in upsert mode for exchanging UPDATE/DELETE messages with the external 
system using the primary key defined on the DDL.

If no primary key is defined on the DDL, the connector can only operate in append mode for 
exchanging INSERT only messages with external system.

Dependencies
------------

In order to use the MongoDB connector the following dependencies are required for both projects 
using a build automation tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.

{{< sql_connector_download_table "mongodb" >}}

The MongoDB connector is not part of the binary distribution. 
See how to link with it for cluster execution [here]({{< ref "docs/dev/configuration/overview" >}}).

How to create a MongoDB table
----------------

The MongoDB table can be defined as following:

```sql
-- register a MongoDB table 'users' in Flink SQL
CREATE TABLE MyUserTable (
  _id STRING,
  name STRING,
  age INT,
  status BOOLEAN,
  PRIMARY KEY (_id) NOT ENFORCED
) WITH (
   'connector' = 'mongodb',
   'uri' = 'mongodb://user:password@127.0.0.1:27017',
   'database' = 'my_db',
   'collection' = 'users'
);

-- write data into the MongoDB table from the other table "T"
INSERT INTO MyUserTable
SELECT _id, name, age, status FROM T;

-- scan data from the MongoDB table
SELECT id, name, age, status FROM MyUserTable;

-- temporal join the MongoDB table as a dimension table
SELECT * FROM myTopic
LEFT JOIN MyUserTable FOR SYSTEM_TIME AS OF myTopic.proctime
ON myTopic.key = MyUserTable._id;
```

Connector Options
----------------

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 25%">Option</th>
        <th class="text-left" style="width: 8%">Required</th>
        <th class="text-left" style="width: 8%">Forwarded</th>
        <th class="text-left" style="width: 7%">Default</th>
        <th class="text-left" style="width: 10%">Type</th>
        <th class="text-left" style="width: 42%">Description</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>connector</h5></td>
      <td>required</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Specify what connector to use, here should be <code>'mongodb'</code>.</td>
    </tr>
    <tr>
      <td><h5>uri</h5></td>
      <td>required</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The MongoDB connection uri.</td>
    </tr>
    <tr>
      <td><h5>database</h5></td>
      <td>required</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The name of MongoDB database to read or write.</td>
    </tr>
    <tr>
      <td><h5>collection</h5></td>
      <td>required</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The name of MongoDB collection to read or write.</td>
    </tr>
    <tr>
      <td><h5>scan.fetch-size</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">2048</td>
      <td>Integer</td>
      <td>Gives the reader a hint as to the number of documents that should be fetched from the database per round-trip when reading.</td>
    </tr>
    <tr>
      <td><h5>scan.cursor.no-timeout</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>MongoDB server normally times out idle cursors after an inactivity period (10 minutes) 
          to prevent excess memory use. Set this option to true to prevent that. However, if the 
          application takes longer than 30 minutes to process the current batch of documents, 
          the session is marked as expired and closed.</td>
    </tr>
    <tr>
      <td><h5>scan.partition.strategy</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">default</td>
      <td>String</td>
      <td>Specifies the partition strategy. Available strategies are `single`, `sample`, `split-vector`, `sharded` and `default`. 
          See the following <a href="#partitioned-scan">Partitioned Scan</a> section for more details.</td>
    </tr>
    <tr>
      <td><h5>scan.partition.size</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">64mb</td>
      <td>MemorySize</td>
      <td>Specifies the partition memory size.</td>
    </tr>
    <tr>
      <td><h5>scan.partition.samples</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">10</td>
      <td>Integer</td>
      <td>Specifies the samples count per partition. It only takes effect when the partition strategy is sample.
          The sample partitioner samples the collection, projects and sorts by the partition fields.
          Then uses every `scan.partition.samples` as the value to use to calculate the partition boundaries.
          The total number of samples taken is calculated as: `samples per partition * (count of documents / number of documents per partition)`.</td>
    </tr>
    <tr>
      <td><h5>lookup.cache</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">NONE</td>
      <td><p>Enum</p>Possible values: NONE, PARTIAL</td>
      <td>The cache strategy for the lookup table. Currently supports NONE (no caching) and PARTIAL (caching entries on lookup operation in external database).</td>
    </tr>
    <tr>
      <td><h5>lookup.partial-cache.max-rows</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Long</td>
      <td>The max number of rows of lookup cache, over this value, the oldest rows will be expired.
      "lookup.cache" must be set to "PARTIAL" to use this option. See the following <a href="#lookup-cache">Lookup Cache</a> section for more details.</td>
    </tr>
    <tr>
      <td><h5>lookup.partial-cache.expire-after-write</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Duration</td>
      <td>The max time to live for each rows in lookup cache after writing into the cache. 
      "lookup.cache" must be set to "PARTIAL" to use this option. See the following <a href="#lookup-cache">Lookup Cache</a> section for more details. </td>
    </tr>
    <tr>
      <td><h5>lookup.partial-cache.expire-after-access</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Duration</td>
      <td>The max time to live for each rows in lookup cache after accessing the entry in the cache.
      "lookup.cache" must be set to "PARTIAL" to use this option. See the following <a href="#lookup-cache">Lookup Cache</a> section for more details. </td>
    </tr>
    <tr>
      <td><h5>lookup.partial-cache.cache-missing-key</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>Whether to store an empty value into the cache if the lookup key doesn't match any rows in the table. 
        "lookup.cache" must be set to "PARTIAL" to use this option.</td>
    </tr>
    <tr>
      <td><h5>lookup.max-retries</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">3</td>
      <td>Integer</td>
      <td>The max retry times if lookup database failed.</td>
    </tr>
    <tr>
      <td><h5>lookup.retry.interval</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">1s</td>
      <td>Duration</td>
      <td>Specifies the retry time interval if lookup records from database failed.</td>
    </tr>
    <tr>
      <td><h5>filter.handling.policy</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">always</td>
      <td>Enum Possible values: always, never</td>
      <td>Fine-grained configuration to control filter push down. 
          Supported policies are:
          <ul>
            <li><code>always</code>: Always push the supported filters to MongoDB.</li>
            <li><code>never</code>: Never push any filters to MongoDB.</li>
          </ul>
      </td>
    </tr>
    <tr>
      <td><h5>sink.buffer-flush.max-rows</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">1000</td>
      <td>Integer</td>
      <td>Specifies the maximum number of buffered rows per batch request.</td>
    </tr>
    <tr>
      <td><h5>sink.buffer-flush.interval</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">1s</td>
      <td>Duration</td>
      <td>Specifies the batch flush interval.</td>
    </tr>
    <tr>
      <td><h5>sink.max-retries</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">3</td>
      <td>Integer</td>
      <td>The max retry times if writing records to database failed.</td>
    </tr>
    <tr>
      <td><h5>sink.retry.interval</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">1s</td>
      <td>Duration</td>
      <td>Specifies the retry time interval if writing records to database failed.</td>
    </tr>
    <tr>
      <td><h5>sink.parallelism</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>Defines the parallelism of the MongoDB sink operator. By default, the parallelism is determined by the framework using the same parallelism of the upstream chained operator.</td>
    </tr>
    <tr>
      <td><h5>sink.delivery-guarantee</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">at-lease-once</td>
      <td><p>Enum</p>Possible values: none, at-least-once</td>
      <td>Optional delivery guarantee when committing. The exactly-once guarantee is not supported yet.</td>
    </tr>
    <tr>
      <td><h5>sink.ordered-writes</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>Defines MongoDB driver option to perform ordered writes. By default, this is true indicating ordered writes.</td>
    </tr>
    <tr>
      <td><h5>sink.bypass-document-validation</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Defines MongoDB driver option to bypass document validation. By default, this is false indicating validation of documents.</td>
    </tr>
    </tbody>
</table>

Features
--------

### Key handling

The MongoDB sink can work in either upsert mode or append mode, depending on whether a primary key 
is defined. If a primary key is defined, the MongoDB sink works in upsert mode which can consume 
queries containing UPDATE/DELETE messages. If a primary key is not defined, the MongoDB sink works 
in append mode which can only consume queries containing INSERT only messages.

In MongoDB the primary key is used to calculate the MongoDB document _id.
Its value must be unique and immutable in the collection, and may be of any 
[BSON Type](https://www.mongodb.com/docs/manual/reference/bson-types/#bson-types) other than an Array. 
If the _id contains subfields, the subfield names cannot begin with a ($) symbol.

There are also some constraints on the primary key index.
Before MongoDB 4.2, the total size of an index entry, which can include structural overhead 
depending on the BSON type, must be less than 1024 bytes.
Starting in version 4.2, MongoDB removes the Index Key Limit.
For more detailed introduction, you can refer to [Index Key Limit](https://www.mongodb.com/docs/manual/reference/limits/#mongodb-limit-Index-Key-Limit).

The MongoDB connector generates a document _id for every row by compositing all primary key 
fields in the order defined in the DDL.

- When there's only a single field in the specified primary key, we convert the field data to bson 
  value as _id of the corresponding document.
- When there's multiple fields in the specified primary key, we convert and composite these fields 
  into a bson document as the _id of the corresponding document.
  For example, if have a primary key statement `PRIMARY KEY (f1, f2) NOT ENFORCED`,
  the extracted _id will be the form like `_id: {f1: v1, f2: v2}`.

Notice that it will be ambiguous if the _id field exists in DDL, but the primary key is not declared as _id.
Either use the _id column as the key, or rename the _id column.

See [CREATE TABLE DDL]({{< ref "docs/dev/table/sql/create" >}}#create-table) for more details about PRIMARY KEY syntax.

### Partitioned Scan

To accelerate reading data in parallel `Source` task instances, Flink provides partitioned scan 
feature for MongoDB collection. The following partition strategies are provided:

- `single`: treats the entire collection as a single partition.
- `sample`: samples the collection and generate partitions which is fast but possibly uneven.
- `split-vector`: uses the splitVector command to generate partitions for non-sharded
  collections which is fast and even. The splitVector permission is required.
- `sharded`: reads `config.chunks` (MongoDB splits a sharded collection into chunks, and the
  range of the chunks are stored within the collection) as the partitions directly. The
  sharded strategy only used for sharded collection which is fast and even. Read permission
  of config database is required.
- `default`: uses sharded strategy for sharded collections otherwise using split vector
  strategy.

### Lookup Cache

MongoDB connector can be used in temporal join as a lookup source (aka. dimension table). 
Currently, only sync lookup mode is supported.

By default, lookup cache is not enabled. You can enable it by setting `lookup.cache` to `PARTIAL`.

The lookup cache is used to improve performance of temporal join the MongoDB connector. 
By default, lookup cache is not enabled, so all the requests are sent to external database.
When lookup cache is enabled, each process (i.e. TaskManager) will hold a cache. 
Flink will lookup the cache first, and only send requests to external database when cache missing, 
and update cache with the rows returned.
The oldest rows in cache will be expired when the cache hit to the max cached rows 
`lookup.partial-cache.max-rows` or when the row exceeds the max time to live specified by 
`lookup.partial-cache.expire-after-write` or `lookup.partial-cache.expire-after-access`.
The cached rows might not be the latest, users can tune expiration options to a smaller value to 
have a better fresh data, but this may increase the number of requests send to database. 
So this is a balance between throughput and correctness.

By default, flink will cache the empty query result for a Primary key, you can toggle the behaviour
by setting `lookup.partial-cache.cache-missing-key` to false.

### Idempotent Writes

MongoDB connector use upsert writing mode `db.connection.update(<query>, <update>, { upsert: true })` 
rather than insert writing mode `db.connection.insert()` if primary key is defined in DDL.
We composite the primary key fields as the document _id which is the reserved primary key of
MongoDB. Use upsert mode to write rows into MongoDB, which provides idempotence.

When using the `INSERT OVERWRITE` statement to write to a MongoDB table, it forces the use of the upsert mode 
to write to MongoDB. Therefore, if the primary key of the MongoDB table is not defined in the DDL, 
the write operation will be rejected.

If there are failures, the Flink job will recover and re-process from last successful checkpoint, 
which can lead to re-processing messages during recovery. The upsert mode is highly recommended as 
it helps avoid constraint violations or duplicate data if records need to be re-processed.

### [Upsert on a sharded collection](https://www.mongodb.com/docs/manual/reference/method/db.collection.updateOne/#upsert-on-a-sharded-collection)

As Mongo Reference says:
> To use db.collection.updateOne() on a sharded collection:
>
> - If you don't specify upsert: true, you must include an exact match on the _id field or target a single shard (such as by including the shard key in the filter).
> - If you specify upsert: true, the filter must include the shard key.
>
> However, documents in a sharded collection can be missing the shard key fields. 
> To target a document that is missing the shard key, you can use the null equality match 
> in conjunction with another filter condition (such as on the _id field).

When upsert into a sharded collection, the value of the shard key needs to be added to the filter. 
For example:
```javascript
db.collection.updateOne(
    {
        _id: ObjectId('<value>'),
        shardKey0: '<value>',
        shardKey1: '<value>'
    },
    { $set: { status: "D" }},
    { upsert: true }
);
```

In Flink SQL, when creating a sink table, the shard keys need to be declared using the `PARTITIONED BY` syntax. 
The values for shard keys will be obtained from each individual record during runtime and added them into the filter.

```sql
CREATE TABLE MySinkTable (
    _id       BIGINT,
    shardKey0 STRING,
    shardKey1 STRING,
    status    STRING,
    PRIMARY KEY (_id) NOT ENFORCED
) PARTITIONED BY (shardKey0, shardKey1) WITH (
    'connector' = 'mongodb',
    'uri' = 'mongodb://user:password@127.0.0.1:27017',
    'database' = 'my_db',
    'collection' = 'users'
);

-- Insert with dynamic partition
INSERT INTO MySinkTable SELECT _id, shardKey0, shardKey1, status FROM T;

-- Insert with static partition
INSERT INTO MySinkTable PARTITION(shardKey0 = 'value0', shardKey1 = 'value1') SELECT 1, 'INIT';

-- Insert with static(shardKey0) and dynamic(shardKey1) partition
INSERT INTO MySinkTable PARTITION(shardKey0 = 'value0') SELECT 1, 'value1' 'INIT';
```
{{< hint warning >}}
LIMITATION: Although the shard key value is no longer immutable in MongoDB 4.2 and later,
it is necessary to ensure that the shard key remains immutable.

Using Flink SQL upsert mode to write to a sharded collection, 
only the updated shard key value can be obtained and 
the original shard key value cannot be provided in the filter
which may cause a duplicate record error.
{{< /hint >}}

### Filters Pushdown

MongoDB supports pushing down simple comparisons and logical filters to optimize queries.
The mappings from Flink SQL filters to MongoDB query operators are listed in the following table.

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Flink SQL filters</th>
        <th class="text-left"><a href="https://www.mongodb.com/docs/manual/reference/operator/query/">MongoDB Query Operators</a></th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>=</code></td>
      <td><code>$eq</code></td>
    </tr>
    <tr>
      <td><code>&lt;&gt;</code></td>
      <td><code>$ne</code></td>
    </tr>
    <tr>
      <td><code>&gt;</code></td>
      <td><code>$gt</code></td>
    </tr>
    <tr>
      <td><code>&gt;=</code></td>
      <td><code>$gte</code></td>
    </tr>
    <tr>
      <td><code>&lt;</code></td>
      <td><code>$lt</code></td>
    </tr>
    <tr>
      <td><code>&lt;=</code></td>
      <td><code>$lte</code></td>
    </tr>
    <tr>
      <td><code>IS NULL</code></td>
      <td><code>$eq : null</code></td>
    </tr>
    <tr>
      <td><code>IS NOT NULL</code></td>
      <td><code>$ne : null</code></td>
    </tr>
    <tr>
      <td><code>OR</code></td>
      <td><code>$or</code></td>
    </tr>
    <tr>
      <td><code>AND</code></td>
      <td><code>$and</code></td>
    </tr>
    </tbody>
</table>

Data Type Mapping
----------------
The field data type mappings from MongoDB BSON types to Flink SQL data types are listed in the following table.

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left"><a href="https://www.mongodb.com/docs/manual/reference/bson-types/#bson-types">MongoDB BSON type</a></th>
        <th class="text-left"><a href="{{< ref "docs/dev/table/types" >}}">Flink SQL type</a></th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>ObjectId</code></td>
      <td><code>STRING</code></td>
    </tr>
    <tr>
      <td><code>String</code></td>
      <td><code>STRING</code></td>
    </tr>
    <tr>
      <td><code>Boolean</code></td>
      <td><code>BOOLEAN</code></td>
    </tr>
    <tr>
      <td><code>Binary</code></td>
      <td><code>BINARY</code><br>
          <code>VARBINARY</code></td>
    </tr>
    <tr>
      <td><code>Int32</code></td>
      <td><code>INTEGER</code></td>
    </tr>
    <tr>
      <td><code>-</code></td>
      <td><code>TINYINT</code><br>
          <code>SMALLINT</code><br>
          <code>FLOAT</code></td>
    </tr>
    <tr>
      <td><code>Int64</code></td>
      <td><code>BIGINT</code></td>
    </tr>
    <tr>
      <td><code>Double</code></td>
      <td><code>DOUBLE</code></td>
    </tr>
    <tr>
      <td><code>Decimal128</code></td>
      <td><code>DECIMAL</code></td>
    </tr>
    <tr>
      <td><code>DateTime</code></td>
      <td><code>TIMESTAMP_LTZ(3)</code></td>
    </tr>
    <tr>
      <td><code>Timestamp</code></td>
      <td><code>TIMESTAMP_LTZ(0)</code></td>
    </tr>
    <tr>
      <td><code>Object</code></td>
      <td><code>ROW</code></td>
    </tr>
    <tr>
      <td><code>Array</code></td>
      <td><code>ARRAY</code></td>
    </tr>
    </tbody>
</table>

For specific types in MongoDB, we use [Extended JSON format](https://www.mongodb.com/docs/drivers/java/sync/current/fundamentals/data-formats/document-data-format-extended-json/) 
to map them to Flink SQL STRING type.

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left"><a href="https://www.mongodb.com/docs/manual/reference/bson-types/#bson-types">MongoDB BSON type</a></th>
        <th class="text-left">Flink SQL STRING</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>Symbol</code></td>
      <td><code>{"_value": {"$symbol": "12"}}</code></td>
    </tr>
    <tr>
      <td><code>RegularExpression</code></td>
      <td><code>{"_value": {"$regularExpression": {"pattern": "^9$", "options": "i"}}}</code></td>
    </tr>
    <tr>
      <td><code>JavaScript</code></td>
      <td><code>{"_value": {"$code": "function() { return 10; }"}}</code></td>
    </tr>
    <tr>
      <td><code>DbPointer</code></td>
      <td><code>{"_value": {"$dbPointer": {"$ref": "db.coll", "$id": {"$oid": "63932a00da01604af329e33c"}}}}</code></td>
    </tr>
    </tbody>
</table>

{{< top >}}
