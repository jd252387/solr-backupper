# solr-backupper

A run-and-exit CLI job that backs up SolrCloud collections. It resolves a configured whitelist of Solr
*aliases* to their underlying collection, triggers the native Solr replication `backup` command on each
shard's leader replica, polls until each shard finishes, writes a JSON summary report, then exits.

## Build

```bash
./gradlew build
```

Java 21 toolchain, single Gradle module (`app`).

## Run

```bash
./gradlew bootRun
# or, against the packaged jar with real config:
java -jar app/build/libs/app.jar --spring.profiles.active=prod
```

Required properties (base config lives in `app/src/main/resources/application.yaml`,
environment-specific overrides in `application-prod.yaml`): `solr.backup.zookeeper`,
`solr.backup.whitelist-aliases`, `solr.backup.backups-mount`, `solr.auth.username`/`password`, etc. See
`SolrBackupConfiguration` for the full property list.

## Live dashboard

By default the app is silent and exits as soon as the run finishes. To watch a run live in a browser and
retry individual failed/timed-out shards, turn the dashboard on:

```bash
java -jar app/build/libs/app.jar \
  --solr.backup.dashboard.enabled=true \
  # ...your other --solr.backup.* overrides
```

or set `solr.backup.dashboard.enabled: true` in a config file/profile.

Once running, open:

```
http://<host>:<server.port>/dashboard.html
```

`server.port` defaults to Spring Boot's usual `8080` — the dashboard is served by the same embedded Tomcat
the app already starts, not a separate server.

What it does:
- Every shard shows up as **Pending** as soon as its topology is resolved, then moves through
  **Running** → **Success** / **Error** / **Timed out**, live.
- The page polls `GET /api/dashboard/status` every few seconds — no WebSockets/SSE.
- Failed or timed-out shards get a **Retry** button (`POST /api/dashboard/shards/{alias}/{shardName}/retry`)
  that re-runs the backup for just that shard, using the leader URL/core name last seen for it (not a fresh
  ZooKeeper lookup — if the leader changed since the run started, retry won't pick that up until the next
  full run).
- Normally the process exits immediately after writing the report. With the dashboard enabled, it instead
  waits `solr.backup.dashboard.shutdown-grace-period` (default `60s`) before exiting, so the final state and
  any retries stay visible for a bit.

Turning the dashboard on is the only opt-in property in `SolrBackupConfiguration` — everything else fails
fast if unset, but this defaults to `false` so upgrading doesn't change behavior for existing headless
invocations (cron, Jenkins, etc.).

## Running against a local SolrCloud in Docker

If your SolrCloud cluster runs in Docker (e.g. the `zookeeper`/`solr1`/`solr2` services in a sibling
`docker-compose.yml`), Solr registers each replica's leader URL using the container's own Docker-internal
hostname/IP. On Docker Desktop (macOS/Windows) those addresses aren't reachable from your host machine, so
running the jar directly on the host will time out trying to reach the leaders.

Run the backupper as a container on the **same Docker network** instead:

```bash
./gradlew build

# make sure an alias exists pointing at the collection you want to back up:
curl "http://localhost:8983/solr/admin/collections?action=CREATEALIAS&name=<alias>&collections=<collection>&wt=json"

docker run --rm --name solr-backupper-run \
  --network <compose-project>_default \
  -p 18080:8080 \
  -v "$(pwd)/app/build/libs/app.jar:/app.jar:ro" \
  -v "<path-to-solr-compose-dir>/backups:/backups" \
  -v /tmp/solr-backupper-reports:/reports \
  eclipse-temurin:21-jre-jammy \
  java -jar /app.jar \
  --server.port=8080 \
  --solr.backup.zookeeper=zookeeper:2181 \
  --solr.backup.whitelist-aliases=<alias> \
  --solr.backup.backups-mount=/backups \
  --solr.backup.report-output-directory=/reports \
  --solr.backup.dashboard.enabled=true
```

Notes:
- `--network <compose-project>_default` is the network Docker Compose creates for the stack (check with
  `docker network ls`).
- The `-v .../backups:/backups` mount **must** be the same host folder the Solr containers mount at
  `/backups` — Solr itself writes the snapshot files, not the backupper, so they need to share that volume.
  Solr also needs `-Dsolr.security.allow.paths=/backups` in its own `SOLR_OPTS` or it will reject the backup
  location with a 400.
- Dashboard: `http://localhost:18080/dashboard.html`; status API: `http://localhost:18080/api/dashboard/status`.
