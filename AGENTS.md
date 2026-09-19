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

# Flink MongoDB Connector AI Agent Instructions

This file provides guidance for AI coding agents working with the Apache Flink MongoDB connector codebase.

## Prerequisites

- Java 11, 17, or 21 (the `main` branch build matrix; release branch `v2.0` also supports Java 8)
- Maven 3.8.6 (there is no Maven wrapper in this repository; use `mvn` directly)
- Git
- Docker (every `*ITCase` starts MongoDB through Testcontainers, including sharded-cluster tests via `MongoShardedContainers`)
- Unix-like environment (Linux, macOS)
- The connector builds against `flink.version` in the root `pom.xml`. A change must also work against every Flink version in `.github/workflows/push_pr.yml` and the `weekly.yml` matrix.
- MongoDB server compatibility is tested via Maven profiles (`mongodb4`, `mongodb5`, `mongodb6`, `mongodb7`, each pinning `mongodb.version` to a specific server release); CI runs the full test suite once per profile.

## Commands

### Build

- Build without tests: `mvn clean package -DskipTests`
- Full build with tests: `mvn clean verify`
- Build against a specific MongoDB server version (what CI does): `mvn clean verify -Pmongodb7`
- Build against another Flink version: `mvn clean verify -Dflink.version=<version>`
- Single module: `mvn clean install -DskipTests -pl flink-connector-mongodb`
- Skip non-essential checks while iterating: `-Dspotless.check.skip=true -Dcheckstyle.skip=true -Drat.skip=true`
- Dependency convergence check (not run by default — `dependency-convergence` is bound to `<phase>none</phase>` in `flink-connector-parent` "as it interacts badly with shade-plugin", and this repository's CI does not invoke it either): `mvn -pl <module> org.apache.maven.plugins:maven-enforcer-plugin:enforce -Denforcer.rules=dependencyConvergence`

### Testing

- `*Test` classes run in the `test` phase, `*ITCase` classes run in the `integration-test` phase.
- Single unit test class: `mvn test -pl flink-connector-mongodb -Dtest=MongoWriterTest`
- Single ITCase: `mvn verify -pl flink-connector-mongodb -Dtest=MongoSinkITCase`
- ArchUnit rules: `mvn test -pl flink-connector-mongodb -Dtest='*ArchitectureTest'`, then check `git status flink-connector-mongodb/archunit-violations/` (see Testing Standards)
- MongoDB in tests comes from `MongoTestUtil` (`createMongoDBContainer()`, `createMongoDBShardedContainers()`), which reads the target server version from the `mongodb.version` system property — set by whichever `mongodbN` profile is active, not hardcoded in test code.
- CI is defined by `apache/flink-connector-shared-utils` (`.github/workflows/ci.yml@ci_utils`); its Maven command line is the reference when a local run differs from CI.

### Code Quality

- Format code: `mvn spotless:apply` (skipped automatically on JDK 21 — google-java-format does not run there; format on 11 or 17)
- Check formatting: `mvn spotless:check`
- Checkstyle: `mvn checkstyle:check` (config: `tools/maven/checkstyle.xml`, suppressions: `tools/maven/suppressions.xml`)
- License headers: `mvn apache-rat:check`
- `japicmp` compares against `japicmp.referenceVersion` from the root `pom.xml` (`<connector version>-<flink version>` format) and only checks `@Public`/`@PublicEvolving` API.

### Documentation

- `docs/content/docs/connectors/` and `docs/content.zh/docs/connectors/` hold the DataStream and Table docs (English and Chinese, same file set). Option tables are hand-written HTML rows — nothing is generated from `ConfigOption` definitions.
- `docs/data/mongodb.yml` only declares the connector's release `version`/`variants` metadata consumed by the Flink docs site build; it is not a source of table option content.

## Repository Structure

### Modules

- `flink-connector-mongodb` — The connector: `MongoSource` (FLIP-27, `Boundedness.BOUNDED` only — this is a batch/bounded source, there is no CDC or unbounded streaming mode), `MongoSink` (Sink V2), and the Table/SQL factory (`MongoDynamicTableFactory`).
- `flink-sql-connector-mongodb` — Shaded SQL jar; relocates `com.mongodb`/`org.bson`.
- `flink-connector-mongodb-e2e-tests` — End-to-end tests against a real Flink distribution.
- `flink-connector-mongodb-table-tests` — Table/SQL-layer integration tests kept in their own module.

There is no `flink-python` module in this repository.

### Key packages in `flink-connector-mongodb/src/main/java`

- `org.apache.flink.connector.mongodb.source` — `@PublicEvolving`: `MongoSource`, `MongoSourceBuilder`. `source.enumerator` / `source.reader` / `source.split` are `@Internal`.
  - `source.enumerator.splitter` implements the partitioning strategies (`PartitionStrategy`, `@PublicEvolving`): `SINGLE` (no split), `SAMPLE` (random-sample based, the default), `SPLIT_VECTOR` (uses the `splitVector` command, standalone/replica-set only), `SHARDED` (reads chunk boundaries from `config.chunks`), `PAGINATION` (skip/limit based, works everywhere including Atlas). Each has its own `MongoXxxSplitter`.
- `org.apache.flink.connector.mongodb.sink` — `@PublicEvolving`: `MongoSink`, `MongoSinkBuilder`, `MongoSerializationSchema`. `sink.writer` (`MongoWriter`, contexts) is `@Internal`.
- `org.apache.flink.connector.mongodb.table` — Table/SQL layer: `MongoDynamicTableFactory` is `@Internal`; `FilterHandlingPolicy` is `@PublicEvolving`. Converters (`BsonToRowDataConverters`, `RowDataToBsonConverters`) and the lookup function (`MongoRowDataLookupFunction`) live here too.
- `org.apache.flink.connector.mongodb.common` — Shared, `@Internal` utilities: `MongoConnectionOptions`, and in `common.utils`: `MongoConstants`, `MongoSerdeUtils`, `MongoUtils`, `MongoValidationUtils`. Check these before writing new helpers.

## Architecture Boundaries

1. **Source (FLIP-27, bounded only).** `MongoSourceEnumerator` implements `SplitEnumerator<MongoSourceSplit, MongoSourceEnumState>` and assigns splits produced up front by a `MongoSplitAssigner`/`MongoSplitters` (splitting happens once at enumerator startup via the configured `PartitionStrategy`, not incrementally). `MongoSourceReader` / `MongoScanSourceSplitReader` read one split (a query range) to completion. There is no offset-commit or incremental-discovery machinery like Kafka's — a split is either fully read or not read at all.
2. **Sink (Sink V2, at-least-once only — no exactly-once/transactions).** `MongoWriter` batches `WriteModel<BsonDocument>` objects from `MongoSerializationSchema` and flushes via `MongoCollection.bulkWrite`. A flush is triggered by whichever comes first: `batchSize`, `batchIntervalMs` (via a background `ScheduledExecutorService`), or a checkpoint (`flush(boolean endOfInput)`, gated by `flushOnCheckpoint`). `write()` blocks on `mailboxExecutor.yield()` while `checkpointInProgress` is true, so no new records are batched mid-flush. A failed flush is stored in a `volatile Exception flushException` and rethrown from the next `write()`/`flush()` call — it is never silently dropped.
3. **Checkpointed state.** `MongoSourceSplitSerializer` (`CURRENT_VERSION = 0`) and `MongoSourceEnumStateSerializer` version the source's split/enumerator state. The sink is stateless (no writer-state serializer) since it has no transactions to recover.
4. **Table layer.** `MongoDynamicTableSource`/`MongoDynamicTableSink` wrap the DataStream connectors; `MongoConnectorOptions` holds every `ConfigOption`. `MongoFilterPushDownVisitor` and `FilterHandlingPolicy` control filter pushdown into the MongoDB query; `MongoShardKeysExtractor`/`MongoPrimaryKeyExtractor` handle upsert/shard-key semantics.
5. **Connector vs Flink.** Production code may depend only on `@Public`/`@PublicEvolving` Flink API outside connector and util packages (ArchUnit rule, see `ProductionCodeArchitectureTest`). Every Flink API used must exist with the same annotation in `flink.version`, because the connector is released for several Flink minor versions.

## Common Change Patterns

### Adding a Table/SQL option

1. Define the `ConfigOption<T>` in `MongoConnectorOptions`
2. Register and validate it in `MongoDynamicTableFactory` / `MongoConfiguration`
3. Add a factory test and, when behaviour changes, an ITCase in `flink-connector-mongodb-table-tests`
4. Add the option row to `docs/content/docs/connectors/table/mongodb.md` and the same file under `docs/content.zh/`
5. Fill in the Release Notes field on the JIRA ticket

### Adding a DataStream builder option

1. Add it to `MongoSourceBuilder` or `MongoSinkBuilder`, with validation in `build()`
2. Add a builder unit test and, when behaviour changes, an ITCase
3. Document it in `docs/content/docs/connectors/datastream/mongodb.md` and the `.zh` copy

### Changing checkpointed state

1. Bump `MongoSourceSplitSerializer.CURRENT_VERSION` or the enumerator state serializer version, and keep a read path for every older version
2. Verify: serializer test with bytes of the previous version

### Bumping `mongodb.driver.version`

1. Check the driver's own compatibility matrix (`mongodb.com/docs/drivers/java/sync/current/reference/compatibility/`) for both the oldest and newest MongoDB server versions this repository tests against (`mongodb4`–`mongodb7` profiles, plus any server version in `MongoTestUtil`) — a driver bump can silently drop support for the older end of that range (verify empirically against a live container, not just by reading the matrix, since a driver can report a version as unsupported while still working, or vice versa).
2. If server support is dropped, this is a breaking change: bump the connector's own major version first as its own `[hotfix] Set version to X.0-SNAPSHOT` commit, then land the driver bump as a separate, normal PR referencing it.
3. Update the affected `mongodbN.version` propert(y/ies) and, if a server line's support is dropped entirely, the corresponding CI profile in `.github/workflows/push_pr.yml`.
4. Verify: `dependency:tree` for new transitive dependencies, the full ITCase suite against every affected `mongodbN` profile.

### Bumping `flink.version` or changing the CI matrix

1. Get consensus on the JIRA ticket first; this changes which Flink versions the branch supports
2. Update `push_pr.yml` and `weekly.yml`
3. Verify: the build against every Flink version in the matrix

### Fixing a flaky test

1. Name the race or ordering that fails, with the CI log excerpt
2. Wait on the condition, never on time
3. Do not add `Thread.sleep`, larger timeouts, retries, or `@Disabled`
4. Verify: run the test repeatedly and state the number of runs in the PR

## Coding Standards

- **Format Java files with Spotless immediately after editing:** `mvn spotless:apply`. Uses google-java-format with AOSP style.
- **Checkstyle:** `tools/maven/checkstyle.xml`. Do not suppress rules; fix the code instead.
- **Apache License 2.0 header** required on all new files (enforced by Apache Rat). Use an HTML comment for markdown files.
- **API stability annotations:** every user-facing class/method needs `@Public`, `@PublicEvolving`, `@Experimental`, or `@Internal`. Most of this connector's public surface (`MongoSource`, `MongoSink`, their builders, the serialization schemas, `PartitionStrategy`, `FilterHandlingPolicy`) is `@PublicEvolving`, not `@Public`.
- **MongoClient lifecycle:** a `MongoWriter` owns exactly one `MongoClient` for its lifetime, created in the constructor and closed in `close()`. Do not create a client per record or per batch.
- **Logging:** use parameterized log statements (SLF4J `{}` placeholders), never string concatenation.
- **`final`** for variables and fields where applicable.
- **Comments:** do not restate what the code does; explain "the why" where it is non-obvious.
- **Reuse existing code.** Before adding a new utility, check `common.utils` (`MongoUtils`, `MongoSerdeUtils`, `MongoValidationUtils`, `MongoConstants`) and `testutils.MongoTestUtil` in tests.
- Full code style guide: https://flink.apache.org/how-to-contribute/code-style-and-quality-preamble/

## Testing Standards

- Add tests for new behavior, covering success, failure, and edge cases.
- Use **JUnit 5** + **AssertJ** assertions.
- **Integration tests:** name classes with the `ITCase` suffix.
- **MongoDB in tests:** use `MongoTestUtil.createMongoDBContainer()` (single-node) or `createMongoDBShardedContainers()` (sharded cluster, needed for `SHARDED`-strategy and shard-key tests), never construct a `MongoDBContainer` directly — this keeps the image version wired to the active `mongodbN` profile via the `mongodb.version` system property.
- **ArchUnit:** the violation stores under `flink-connector-mongodb/archunit-violations/` are frozen. A local run updates them when violations disappear; commit removed lines together with the change, never add lines.
- Follow the testing conventions at https://flink.apache.org/how-to-contribute/code-style-and-quality-common/#7-testing

## Commits and PRs

### Commit message format

- `[FLINK-XXXX][component] Description` where FLINK-XXXX is the JIRA issue number
- `[hotfix][component] Description` for typo fixes or version-string-only changes without JIRA
- Each commit must have a meaningful message including the JIRA ID. If you don't know the ticket number, ask.
- Separate cleanup/refactoring from functional changes into distinct commits
- When AI tools were used: add `Generated-by: <Tool Name and Version>` trailer per [ASF generative tooling guidance](https://www.apache.org/legal/generative-tooling.html)

### Pull request conventions

- Title format: `[FLINK-XXXX][connectors/mongodb] Title of the pull request`
- A corresponding JIRA issue is required (except hotfixes for typos or version bumps)
- Fill out the PR template completely but concisely
- Each PR should address exactly one issue
- Ensure `mvn clean verify` passes before opening a PR
- Always push to your fork, not directly to `apache/flink-connector-mongodb`
- Branches: `main` is the next major connector version; `v<major>.<minor>` are release branches. Artifacts are versioned `<connector version>-<flink major.minor>`.
- For user-visible behaviour changes, breaking changes, or new options: fill in the **Release Notes** field on the JIRA ticket.

### AI-assisted contributions

- Disclose AI usage by checking the AI disclosure checkbox and filling in the `Generated-by` line in the PR template
- Add `Generated-by: <Tool Name and Version>` to commit messages
- Never add `Co-Authored-By` with an AI agent as co-author; agents are assistants, not authors
- You must be able to explain the design, code, and tests, debug them, and respond to review feedback substantively
- Reviewer-ready quality bar: the author owns PR quality. PRs that look AI-generated without author refinement will be closed without review

## Boundaries

### Ask first

- Adding or changing `@Public`, `@PublicEvolving`, or `@Experimental` API, including Table options. A new source or sink, or a change of connector-wide semantics, requires a FLIP.
- Changes to checkpointed state or its serializers
- Changes to the sink's flush/checkpoint gating in `MongoWriter`
- Bumping `mongodb.driver.version`, `flink.version`, or changing the CI matrix
- New dependencies

### Never

- Commit secrets, credentials, or connection strings
- Push directly to `apache/flink-connector-mongodb`; always work from your fork
- Mix unrelated changes into one PR
- Construct a `MongoDBContainer`/`MongoShardedContainers` directly in a test instead of going through `MongoTestUtil`
- Use `@Internal` Flink classes, or Flink API that does not exist in `flink.version`
- Add lines to `flink-connector-mongodb/archunit-violations/` or the RAT excludes
- Add `Co-Authored-By` with an AI agent as co-author in commit messages; use `Generated-by: <Tool Name and Version>` instead
- Use destructive git operations unless explicitly requested

## References

- [README.md](README.md) — Build instructions and project overview
- [.github/PULL_REQUEST_TEMPLATE.md](.github/PULL_REQUEST_TEMPLATE.md) — PR checklist
- [MongoDB connector documentation](https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/datastream/mongodb/) — User-facing docs
- [Externalized Connector development](https://cwiki.apache.org/confluence/display/FLINK/Externalized+Connector+development) — Versioning, branching, Flink compatibility, and common review issues for connector repositories
- [Code Style Guide](https://flink.apache.org/how-to-contribute/code-style-and-quality-preamble/) — Detailed coding guidelines
- [Flink Improvement Proposals](https://cwiki.apache.org/confluence/display/FLINK/Flink+Improvement+Proposals) — When a FLIP is required
- [ASF Generative Tooling Guidance](https://www.apache.org/legal/generative-tooling.html) — AI tooling policy
