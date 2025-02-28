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

# MongoDB SQL 连接器

{{< label "Scan Source: Bounded" >}}
{{< label "Lookup Source: Sync Mode" >}}
{{< label "Sink: Batch" >}}
{{< label "Sink: Streaming Append & Upsert Mode" >}}

MongoDB 连接器提供了从 MongoDB 中读取和写入数据的能力。
本文档介绍如何设置 MongoDB 连接器以对 MongoDB 运行 SQL 查询。

连接器可以在 upsert 模式下运行，使用在 DDL 上定义的主键与外部系统交换 UPDATE/DELETE 消息。

如果 DDL 上没有定义主键，则连接器只能以 append 模式与外部系统交换 INSERT 消息且不支持消费 UPDATE/DELETE 消息。

依赖
------------
In order to use the MongoDB connector the following dependencies are required for both projects 
using a build automation tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.

{{< sql_connector_download_table "mongodb" >}}

MongoDB 连接器目前并不包含在 Flink 的二进制发行版中，请查阅[这里]({{< ref "docs/dev/configuration/overview" >}})了解如何在集群运行中引用 MongoDB 连接器。

如何创建 MongoDB 表
----------------

MongoDB 表可以以如下方式定义:

```sql
-- 在 Flink SQL 中注册一张 MongoDB 'users' 表
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

-- 读取表 "T" 的数据并写入至 MongoDB 表
INSERT INTO MyUserTable
SELECT _id, name, age, status FROM T;

-- 从 MongoDB 表中读取数据
SELECT id, name, age, status FROM MyUserTable;

-- 将 MongoDB 表作为维度表关联
SELECT * FROM myTopic
LEFT JOIN MyUserTable FOR SYSTEM_TIME AS OF myTopic.proctime
ON myTopic.key = MyUserTable._id;
```

连接器参数
----------------

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 25%">参数</th>
        <th class="text-left" style="width: 8%">是否必选</th>
        <th class="text-left" style="width: 8%">是否透传</th>
        <th class="text-left" style="width: 7%">默认值</th>
        <th class="text-left" style="width: 10%">数据类型</th>
        <th class="text-left" style="width: 42%">描述</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>connector</h5></td>
      <td>必填</td>
      <td>否</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>指定使用什么类型的连接器，这里应该是<code>'mongodb'</code>。</td>
    </tr>
    <tr>
      <td><h5>uri</h5></td>
      <td>必填</td>
      <td>是</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>MongoDB 连接字符串。</td>
    </tr>
    <tr>
      <td><h5>database</h5></td>
      <td>必填</td>
      <td>是</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>读取或写入的数据库。</td>
    </tr>
    <tr>
      <td><h5>collection</h5></td>
      <td>必填</td>
      <td>是</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>读取或写入的集合。</td>
    </tr>
    <tr>
      <td><h5>scan.fetch-size</h5></td>
      <td>可选</td>
      <td>是</td>
      <td style="word-wrap: break-word;">2048</td>
      <td>Integer</td>
      <td>每次循环读取时应该从游标中获取的行数。</td>
    </tr>
    <tr>
      <td><h5>scan.cursor.no-timeout</h5></td>
      <td>可选</td>
      <td>是</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>MongoDB 服务端通常会将空闲时间超过 10 分钟的 cursor 关闭，来节省内存开销。将这个参数设置为 true 可以
          防止 cursor 因为读取时间过长或者背压导致的空闲而关闭。需要注意的是。如果 cursor 所在的会话空闲时间超过
          了 30 分钟，MongoDB 会关闭当前会话以及由当前会话打开的 cursors，不管有没有设置 `noCursorTimeout()` 
          或者 `maxTimeMS()` 大于 30分钟。</td>
    </tr>
    <tr>
      <td><h5>scan.partition.strategy</h5></td>
      <td>可选</td>
      <td>否</td>
      <td style="word-wrap: break-word;">default</td>
      <td>String</td>
      <td>设置分区策略。可以选择的分区策略有`single`，`sample`，`split-vector`，`sharded` 和 `default`。 
          请参阅<a href="#分区扫描">分区扫描</a>部分了解更多详情。</td>
    </tr>
    <tr>
      <td><h5>scan.partition.size</h5></td>
      <td>可选</td>
      <td>否</td>
      <td style="word-wrap: break-word;">64mb</td>
      <td>MemorySize</td>
      <td>设置每个分区的内存大小。通过指定的分区大小，将 MongoDB 的一个集合切分成多个分区。</td>
    </tr>
    <tr>
      <td><h5>scan.partition.samples</h5></td>
      <td>可选</td>
      <td>否</td>
      <td style="word-wrap: break-word;">10</td>
      <td>Integer</td>
      <td>仅用于 `SAMPLE` 抽样分区策略，设置每个分区的样本数量。抽样分区器根据分区键对集合进行随机采样的方式计算分区边界。
          `总的样本数量 = 每个分区的样本数量 * (文档总数 / 每个分区的文档数量)`。</td>
    </tr>
    <tr>
      <td><h5>lookup.cache</h5></td>
      <td>可选</td>
      <td>否</td>
      <td style="word-wrap: break-word;">NONE</td>
      <td><p>枚举类型</p>可选值: NONE, PARTIAL</td>
      <td>维表的缓存策略。 目前支持 NONE（不缓存）和 PARTIAL（只在外部数据库中查找数据时缓存）。</td>
    </tr>
    <tr>
      <td><h5>lookup.partial-cache.max-rows</h5></td>
      <td>可选</td>
      <td>否</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Long</td>
      <td>维表缓存的最大行数，若超过该值，则最老的行记录将会过期。
          使用该配置时 "lookup.cache" 必须设置为 "PARTIAL”。请参阅下面的 <a href="#lookup-cache">Lookup Cache</a> 部分了解更多详情。</td>
    </tr>
    <tr>
      <td><h5>lookup.partial-cache.expire-after-write</h5></td>
      <td>可选</td>
      <td>否</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Duration</td>
      <td>在记录写入缓存后该记录的最大保留时间。使用该配置时 "lookup.cache" 必须设置为 "PARTIAL”。
          请参阅下面的 <a href="#lookup-cache">Lookup Cache</a> 部分了解更多详情。</td>
    </tr>
    <tr>
      <td><h5>lookup.partial-cache.expire-after-access</h5></td>
      <td>可选</td>
      <td>否</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Duration</td>
      <td>在缓存中的记录被访问后该记录的最大保留时间。
          使用该配置时 "lookup.cache" 必须设置为 "PARTIAL”。
          请参阅下面的 <a href="#lookup-cache">Lookup Cache</a> 部分了解更多详情。</td>
    </tr>
    <tr>
      <td><h5>lookup.partial-cache.cache-missing-key</h5></td>
      <td>可选</td>
      <td>否</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>是否缓存维表中不存在的键，默认为true。
          使用该配置时 "lookup.cache" 必须设置为 "PARTIAL”。</td>
    </tr>
    <tr>
      <td><h5>lookup.max-retries</h5></td>
      <td>可选</td>
      <td>否</td>
      <td style="word-wrap: break-word;">3</td>
      <td>Integer</td>
      <td>查询数据库失败的最大重试次数。</td>
    </tr>
    <tr>
      <td><h5>lookup.retry.interval</h5></td>
      <td>可选</td>
      <td>否</td>
      <td style="word-wrap: break-word;">1s</td>
      <td>Duration</td>
      <td>查询数据库失败的最大重试时间。</td>
    </tr>
    <tr>
      <td><h5>filter.handling.policy</h5></td>
      <td>可选</td>
      <td>否</td>
      <td style="word-wrap: break-word;">always</td>
      <td>枚举值，可选项: always, never</td>
      <td>过滤器下推策略，支持的策略有:
          <ul>
            <li><code>always</code>: 始终将支持的过滤器下推到数据库.</li>
            <li><code>never</code>: 不将任何过滤器下推到数据库.</li>
          </ul>
      </td>
    </tr>
    <tr>
      <td><h5>sink.buffer-flush.max-rows</h5></td>
      <td>可选</td>
      <td>是</td>
      <td style="word-wrap: break-word;">1000</td>
      <td>Integer</td>
      <td>设置写入的最大批次大小。</td>
    </tr>
    <tr>
      <td><h5>sink.buffer-flush.interval</h5></td>
      <td>可选</td>
      <td>是</td>
      <td style="word-wrap: break-word;">1s</td>
      <td>Duration</td>
      <td>设置写入的最大间隔时间。</td>
    </tr>
    <tr>
      <td><h5>sink.max-retries</h5></td>
      <td>可选</td>
      <td>是</td>
      <td style="word-wrap: break-word;">3</td>
      <td>Integer</td>
      <td>设置写入失败时最大重试次数。</td>
    </tr>
    <tr>
      <td><h5>sink.retry.interval</h5></td>
      <td>可选</td>
      <td>是</td>
      <td style="word-wrap: break-word;">1s</td>
      <td>Duration</td>
      <td>设置写入失败时最大重试时间。</td>
    </tr>
    <tr>
      <td><h5>sink.parallelism</h5></td>
      <td>可选</td>
      <td>否</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>用于定义 MongoDB sink 算子的并行度。默认情况下，并行度是由框架决定：使用与上游链式算子相同的并行度。</td>
    </tr>
    <tr>
      <td><h5>sink.delivery-guarantee</h5></td>
      <td>可选</td>
      <td>否</td>
      <td style="word-wrap: break-word;">at-lease-once</td>
      <td><p>Enum</p>可选值: none, at-least-once</td>
      <td>设置投递保证。仅一次（exactly-once）的投递保证暂不支持。</td>
    </tr> 
    </tbody>
</table>

特性
--------

### 键处理
当写入数据到外部数据库时，Flink 会使用 DDL 中定义的主键。
如果定义了主键，则连接器将以 upsert 模式工作，否则连接器将以 append 模式工作。

在 MongoDB 连接器中主键用来计算 MongoDB 文档的保留主键 _id。
MongoDB 的文档 _id 是唯一的且不可变的，可以是除了 Array 外的任何 [BSON 类型](https://www.mongodb.com/docs/manual/reference/bson-types/#bson-types)。
如果 _id 是嵌套的，嵌套字段的名称不能包含 ($) 符号。

MongoDB 的主键有一些限制。
在 MongoDB 4.2 之前，索引值的大小需要小于 1024 字节。
在 MongoDB 4.2 及之后版本，移除了索引大小的限制。
请参阅 [Index Key Limit](https://www.mongodb.com/docs/manual/reference/limits/#mongodb-limit-Index-Key-Limit) 了解更多详情。

MongoDB 连接器通过将 DDL 声明的主键进行组合，来生成文档的保留主键 _id。

- 当 DDL 中声明的主键仅包含一个字段时，将字段数据转换为 bson 类型的值作为相应文档的_id。
- 当 DDL 中声明的主键包含多个字段时，我们将所有声明的主键字段组合为一个 bson document 的嵌套类型，作为相应文档的_id。
  举个例子，如果我们在 DDL 中定义主键为 `PRIMARY KEY (f1, f2) NOT ENFORCED`，那么相应文档的 _id 将会是 `_id: {f1: v1, f2: v2}` 的形式。

需要注意的是：当 _id 字段存在于 DDL 中，但是主键并没有指定为 _id 时，会产生歧义。
要么将 _id 字段声明为主键，或者将 _id 字段进行重命名。

有关 PRIMARY KEY 语法的更多详细信息，请参见 [CREATE TABLE DDL]({{< ref "docs/dev/table/sql/create" >}}#create-table)。

### 分区扫描
为了在并行 `Source` task 实例中加速读取数据，Flink 为 MongoDB table 提供了分区扫描的特性。

- `single`: 将整个集合作为一个分区。
- `sample`: 通过随机采样的方式来生成分区，快速但可能不均匀。
- `split-vector`: 通过 MongoDB 计算分片的 splitVector 命令来生成分区，快速且均匀。
  仅适用于未分片集合，需要 splitVector 权限。
- `sharded`: 从 `config.chunks` 集合中直接读取分片集合的分片边界作为分区，不需要额外计算，快速且均匀。
  仅适用于已经分片的集合，需要 config 数据库的读取权限。
- `default`: 对分片集合使用 `sharded` 策略，对未分片集合使用 `split-vector` 策略。

### Lookup Cache

MongoDB 连接器可以用在时态表关联中作为一个可 lookup 的 source (又称为维表)，当前只支持同步的查找模式。

默认情况下，lookup cache 是未启用的，你可以将 `lookup.cache` 设置为 `PARTIAL` 参数来启用。

lookup cache 的主要目的是用于提高时态表关联 MongoDB 连接器的性能。默认情况下，lookup cache 不开启，所以所有请求都会发送到外部数据库。
当 lookup cache 被启用时，每个进程（即 TaskManager）将维护一个缓存。Flink 将优先查找缓存，只有当缓存未查找到时才向外部数据库发送请求，并使用返回的数据更新缓存。
当缓存命中最大缓存行 `lookup.partial-cache.max-rows` 或当行超过 `lookup.partial-cache.expire-after-write` 或 `lookup.partial-cache.expire-after-access` 指定的最大存活时间时，缓存中的行将被设置为已过期。
缓存中的记录可能不是最新的，用户可以将缓存记录超时设置为一个更小的值以获得更好的刷新数据，但这可能会增加发送到数据库的请求数。所以要做好吞吐量和正确性之间的平衡。

默认情况下，flink 会缓存主键的空查询结果，你可以通过将 `lookup.partial-cache.cache-missing-key` 设置为 false 来切换行为。

### 幂等写入
如果在 DDL 中定义了主键，MongoDB connector 将使用 UPSERT 模式 `db.connection.update(<query>, <update>, { upsert: true })` 写入 MongoDB 
而不是 INSERT 模式 `db.connection.insert()`。 我们将 DDL 中声明的主键进行组合作为 MongoDB 保留主键 _id，使用 UPSERT 模式进行写入，来保证写入的幂等性。

当使用 `INSERT OVERWRITE` 写入 MongoDB Table 时，会强制使用 UPSERT 模式写入 MongoDB。
因此，当DDL中没有定义 MongoDB Table 的主键时，会拒绝写入。

如果出现故障，Flink 作业会从上次成功的 checkpoint 恢复并重新处理，这可能导致在恢复过程中重复处理消息。
强烈推荐使用 UPSERT 模式，因为如果需要重复处理记录，它有助于避免违反数据库主键约束和产生重复数据。

### [Upsert 写入分片集合](https://www.mongodb.com/docs/manual/reference/method/db.collection.updateOne/#upsert-on-a-sharded-collection)

在 Mongo 文档中提到:
> To use db.collection.updateOne() on a sharded collection:
>
> - If you don't specify upsert: true, you must include an exact match on the _id field or target a single shard (such as by including the shard key in the filter).
> - If you specify upsert: true, the filter must include the shard key.
>
> However, documents in a sharded collection can be missing the shard key fields.
> To target a document that is missing the shard key, you can use the null equality match
> in conjunction with another filter condition (such as on the _id field).

当使用 upsert 模式写入分片集合时，需要将分片键的值添加到 filter 中， 如：
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

使用 Flink SQL 创建 sink 表映射分片集合时，需要使用 `PARTITIONED BY` 语法声明分片键。
分片键的值将在运行时从每个单独的记录中获取，并将其添加到 filter 中。

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

-- 动态写入分片集合
INSERT INTO MySinkTable SELECT _id, shardKey0, shardKey1, status FROM T;

-- 指定固定分片键的值
INSERT INTO MySinkTable PARTITION(shardKey0 = 'value0', shardKey1 = 'value1') SELECT 1, 'INIT';

-- 指定固定分片键值 (shardKey0) 和动态分片键值 (shardKey1) 
INSERT INTO MySinkTable PARTITION(shardKey0 = 'value0') SELECT 1, 'value1' 'INIT';
```
{{< hint warning >}}
限制：尽管 MongoDB 4.2 及之后版本中分片键的值不再是不可变的，
使用 MongoDB Connector upsert 写入分片集合需要确保分片键的值保持不可变。
因为在 upsert 模式下，只能获取更新后的分片键值，无法获取原始分片键的值添加至 filter 中，
这可能导致重复记录的错误。
{{< /hint >}}

### 过滤器下推

MongoDB 支持将 Flink SQL 的简单比较和逻辑过滤器下推以优化查询。
Flink SQL 过滤器到 MongoDB 查询操作符的映射如下表所示。

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

数据类型映射
----------------
MongoDB BSON 类型到 Flink SQL 数据类型的映射如下表所示。

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

对于 MongoDB 独有的类型，我们使用 [Extended JSON 格式](https://www.mongodb.com/docs/drivers/java/sync/current/fundamentals/data-formats/document-data-format-extended-json/)
将其映射为 Flink SQL STRING 类型。

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
