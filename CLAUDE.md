# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A CLI application (single Spring Boot run-and-exit job, not a server) that backs up SolrCloud collections.
It resolves a configured whitelist of Solr *aliases* to their underlying collection, triggers the native
Solr replication `backup` command on each shard's leader replica, polls until each shard's backup finishes,
then writes a JSON summary report and exits.

A live report file is rewritten to disk on a configurable interval throughout the run; a separate
containerized React dashboard (`../solr-backup-dashboard`) reads that file off a shared volume to display
runs live. See "Reports & live dashboard" below.

## Commands

- Build: `./gradlew build`
- Run all tests: `./gradlew test`
- Run a single test class: `./gradlew test --tests "org.example.report.BackupReportWriterTest"`
- Run a single test method: `./gradlew test --tests "org.example.report.BackupReportWriterTest.buildsReportWithCountsListsAndPerAliasAverages"`
- Run the app: `./gradlew bootRun` (or run the built jar with `--spring.profiles.active=prod`)

Java toolchain is pinned to 21 (`app/build.gradle.kts`). Tests use JUnit 4 (`org.junit.Test`), not JUnit 5.

## Architecture

Everything lives under `app/src/main/java/org/example/`. There is a single Gradle module (`app`).

- **`Main`** — Spring Boot entry point (`@SpringBootApplication`). The app runs headless
  (`spring.main.web-application-type: none`): no embedded web server is started. There is no servlet/web
  starter on the classpath — only `spring-webflux` + `reactor-netty`, and purely so `WebClient` can act as
  an HTTP client against Solr's replication handler; nothing is served over HTTP.
- **`Runner`** (`ApplicationRunner`) — the entire backup pipeline. On startup it:
  1. Connects to ZooKeeper via `CloudSolrClient` to read cluster state and the alias → collection(s) map.
  2. For each whitelisted alias (`solr.backup.whitelist-aliases`), resolves it to exactly one collection
     (aliases pointing to zero, multiple collections, or missing collections are logged and skipped —
     `resolveAlias`).
  3. Registers **every** shard of every resolved collection as `PENDING` in `DashboardState` up front and
     writes an initial report immediately, so the dashboard shows the whole run (all shards pending) from
     the start rather than shards appearing only once they are already running.
  4. Then backs up the shards, staggering aliases by `between-aliases-delay` (default 15s) to spread load
     (the first alias starts immediately), delegating each shard to `ShardBackupExecutor.backupShard`
     (see below).
  5. Shard backups run with bounded concurrency (`solr.backup.parallel-backups`) via `Flux.flatMap`, on
     `Schedulers.boundedElastic()`.
  6. Throughout the run a daemon scheduler calls `BackupReportWriter.writeReport()` every
     `solr.backup.report-update-interval`, rewriting the live report file from `DashboardState`. When the
     pipeline completes it stops the scheduler, marks the run finished, writes one final report, and
     `System.exit(0)`s — pure run-and-exit, no keep-alive.
- **`ShardBackupExecutor`** — executes one shard's backup: creates the on-disk backup directory
  (`{backups-mount}/{alias}/{shard}`), calls the shard leader replica's native Solr replication handler
  (`?command=backup&location=...` to start, then repeatedly polls `?command=details` until the reported
  backup has `status=success`). This **requires the patched Solr build** (`../solr`, commit `757b3e0`), which
  publishes `details.backup` while a backup runs rather than only after it finishes: `waiting for commit`
  (emitted synchronously, before `command=backup` returns) and then `running` with `fileCount` /
  `finishedFileCount` after every copied file. Both mean "keep polling"; `success` means finished; anything
  else — including the `{exception: ...}` payload — throws. Because the new backup's status is visible before
  the trigger call returns, a previous backup's `success` can no longer be mistaken for ours, so no
  start-time comparison and no initial poll delay are needed. Each `running` report's file counts are
  recorded onto the shard's current attempt (`DashboardState.updateAttemptProgress`) and reach the report as
  live per-attempt progress. Before triggering, it polls `?command=details` once and refuses to start (a
  retryable `A backup is already in progress on this core` error) if a backup is still `waiting for commit`
  or `running`, because Solr does not serialize concurrent backups — a second `command=backup` would run in
  parallel against a single shared status field. There is **no timeout** — polling continues
  until the core reports success or failure. If a poll reports no backup at all (and none has finished),
  that is treated as a failure with the message `Core stopped updating on backup at <time>`. A failed
  attempt is retried up to `retries` (default 2) times (`attemptBackup` wrapped in `Retry.fixedDelay`, waiting `retry-delay` — default 20s — between attempts); a shard
  that succeeds on any attempt is `SUCCESS`, and only a shard that fails every attempt is `ERROR`. **Each
  attempt resolves the shard's leader afresh** through the `Supplier<Replica>` `Runner` passes in
  (`Runner.currentLeader` re-reads cluster state), because a retry usually follows the very failure that
  moved the leader — retrying against the replica that led when the run started would keep hitting a node
  that no longer leads. The resolved leader is written back onto the shard
  (`DashboardState.updateLeader`) so the report names the core actually used; a shard with no elected
  leader fails the attempt (`Shard has no elected leader`) and retries. Every
  attempt is recorded in `DashboardState` (`startAttempt`/`finishAttempt`, each with its own
  start/finish/error/file progress), and each failed attempt's partial snapshot is deleted from disk
  (`deleteFailedBackup`: the exact `snapshot.*` directory Solr reported in `directoryName`, under
  `{backups-mount}/{alias}/{shard}`, leaving any earlier good snapshots intact), logging
  `Deleted failed backup` with the removed path so failed replications don't linger on the mount. A failing
  shard never fails the whole run — one bad shard never blocks the rest. Takes plain identifying strings
  (alias/collection/shard) plus the leader supplier, rather than a live Solr `Slice`.
- **`configuration/SolrBackupConfiguration`** — `@ConfigurationProperties("solr.backup")`: zookeeper
  connection string, alias whitelist, backups mount path, parallelism, poll interval (`status-every`),
  and report output directory.
  `report-update-interval` (the live-write cadence), `between-aliases-delay` (the stagger between aliases,
  default 15s), and `retries` (per-shard backup retries, default 2) carry in-code defaults so existing
  config still binds. `status-every` also sets how often live file progress refreshes.
  The report's `cluster` / `environment` are **not**
  config properties — both are taken from the active Spring profile (`spring.profiles.active`) in
  `DashboardState`, so there is a single source of truth.
- **`configuration/WebConfiguration`** — builds the shared `WebClient` (Reactor Netty connector, HTTP Basic
  auth from `solr.auth.username`/`solr.auth.password`) and the shared `ObjectMapper` (with
  `findAndRegisterModules()` for `Instant` support) used for parsing Solr's replication-status JSON and
  writing the report.
- **`dashboard/`** — the live in-memory model the report is built from (no longer serves HTTP):
  - `DashboardState` — thread-safe, always-on in-memory map of every shard's live `ShardState`
    (alias/collection/shard/core/leader URL/status/timestamps/error), updated by `ShardBackupExecutor` at
    every transition. `BackupReportWriter` snapshots it to build the on-disk `RunReport`.
  - `ShardState` / `ShardStatus` / `ShardAttempt` — the per-shard record (overall four-state lifecycle
    `PENDING`/`RUNNING`/`SUCCESS`/`ERROR`) plus a `ShardAttempt` list, one entry per backup attempt
    (initial try + retries), each with its own status/start/finish/error and the `fileCount` /
    `finishedFileCount` Solr reports while copying (null until the core resolves its file list, so a failed
    attempt keeps the progress it died at; a succeeded attempt is completed to `fileCount`, since polling
    usually misses Solr's final progress report).
- **`report/`** — the live report, kept separate from orchestration in `Runner`:
  - `RunReport` (+ nested `Counts`, `CollectionReport`, `ShardReport` with per-attempt `Attempt` rows) and
    `RunStatus` (`ACTIVE`/`SUCCESS`/`ERROR`) — the run-scoped, per-shard model serialized to disk. Each
    `ShardReport` carries the shard's overall status plus every backup attempt (initial try + retries), so
    the file captures the full lifecycle (including `PENDING`/`RUNNING`) and can be rewritten repeatedly and
    read live. A finished run is `ERROR` if any shard errored, otherwise `SUCCESS` (a shard that succeeds on
    a retry counts as `SUCCESS`).
  - `BackupReportWriter.buildReport` — builds a `RunReport` from `DashboardState.snapshot()` + config:
    groups shards by alias/collection, counts by status, derives `RunStatus`, and sets `cluster` /
    `environment` from `DashboardState.clusterName()` (the active Spring profile).
  - `BackupReportWriter.writeReport` — serializes it to a **stable** path
    `{report-output-directory}/backup-report-{runId}.json`, replacing it via a temp file + `ATOMIC_MOVE` so
    a concurrent reader never sees a torn file. A write failure is logged, not thrown — reporting must never
    fail an otherwise-successful backup run.

## Configuration profiles

- `application.yaml` — base config: `spring.main.web-application-type: none` (headless), backup tuning
  (parallelism, mount path, timeouts, report directory, `report-update-interval`) and Solr auth credentials.
- `application-prod.yaml` (activate with profile `prod`) — environment-specific overrides: the ZooKeeper
  connection string and the alias whitelist. The active profile name (`prod`) is itself used as the
  report's `cluster` / `environment`, so there are no separate keys for those.

Both `solr.backup.*` and `solr.auth.*` values are required at startup with no defaults in code — missing
properties will fail Spring's configuration-properties binding. The deliberate exceptions are
`report-update-interval`, `between-aliases-delay`, and `retries`, which carry
in-code defaults so existing deployments keep binding without config changes. The report's `cluster` /
`environment` are not `solr.backup.*` properties at all — they come from `spring.profiles.active`.

## Reports & live dashboard

The report is the integration point. `Runner` rewrites `{report-output-directory}/backup-report-{runId}.json`
every `solr.backup.report-update-interval` (default 5s) for the whole run — atomically, snapshotting
`DashboardState` — then writes one final copy and exits. Each file is a self-contained `RunReport` (run
metadata + counts + collections → shards, including `PENDING`/`RUNNING` shards).

The UI is a **separate project**, `../solr-backup-dashboard` (React + Mantine + a thin Node/Express
backend), deployed as its own container. It mounts the report directory as a shared read-only volume, scans
it, and serves two views: a runs list across clusters, and a live run-detail view (collections → shards).
The two processes never talk over HTTP — they only share the report path. Because each report carries
`cluster` (the active Spring profile), several backupper instances (each run with a different
`spring.profiles.active`) can write into one shared directory and the dashboard groups/filters their runs
by cluster.

There is no "backup size" column: Solr's replication `command=details` response doesn't reliably expose
backup size, so it's omitted rather than faked.

## Conventions

- Every source file starts with a `/* (C)Team Eclipse 2024 */` header — keep this on new files.
- Lombok (`@Data`, `@RequiredArgsConstructor`, `@Slf4j`) is used throughout; prefer it over hand-written
  boilerplate to stay consistent.
- Logging uses SLF4J's fluent API (`log.atInfo().setMessage(...).addKeyValue(...).log()`) with structured
  key-value pairs (shard_name, core_name, leader_url, alias, collection, etc.), not string interpolation —
  match this style for new log statements.
