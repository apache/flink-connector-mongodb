--/*
-- * Licensed to the Apache Software Foundation (ASF) under one
-- * or more contributor license agreements.  See the NOTICE file
-- * distributed with this work for additional information
-- * regarding copyright ownership.  The ASF licenses this file
-- * to you under the Apache License, Version 2.0 (the
-- * "License"); you may not use this file except in compliance
-- * with the License.  You may obtain a copy of the License at
-- *
-- *     http://www.apache.org/licenses/LICENSE-2.0
-- *
-- * Unless required by applicable law or agreed to in writing, software
-- * distributed under the License is distributed on an "AS IS" BASIS,
-- * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- * See the License for the specific language governing permissions and
-- * limitations under the License.
-- */

DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS orders_bak;

CREATE TABLE orders (
    `_id` STRING,
    `code` STRING,
    `quantity` BIGINT,
    PRIMARY KEY (_id) NOT ENFORCED
) WITH (
    'connector' = 'mongodb',
    'uri' = 'mongodb://mongodb:27017',
    'database' = 'test_unbounded',
    'collection' = 'orders',
    'scan.startup.mode' = 'initial'
);

CREATE TABLE orders_bak (
    `_id` STRING,
    `code` STRING,
    `quantity` BIGINT,
    PRIMARY KEY (_id) NOT ENFORCED
) WITH (
    'connector' = 'mongodb',
    'uri' = 'mongodb://mongodb:27017',
    'database' = 'test_unbounded',
    'collection' = 'orders_bak'
);

INSERT INTO orders_bak SELECT * FROM orders;
