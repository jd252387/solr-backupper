/* (C)Team Eclipse 2024 */
package org.example.dashboard;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

/**
 * Thread-safe, in-memory view of every shard's live backup status for the current run. Kept as a plain
 * always-on bean so {@link org.example.ShardBackupExecutor} can update it unconditionally at negligible
 * cost; {@link org.example.report.BackupReportWriter} snapshots it to build the on-disk report.
 *
 * <p>Each shard tracks an overall status plus a list of individual attempts (initial try + retries):
 * {@link #startAttempt}/{@link #finishAttempt} record each attempt, and {@link #markShardResult} sets the
 * shard's terminal outcome once all attempts are done.
 */
@Service
public class DashboardState {
    private static final char KEY_SEPARATOR = ' ';

    /** Filesystem- and URL-safe UTC timestamp, e.g. {@code 2026-07-06T18-30-00Z}. */
    private static final DateTimeFormatter RUN_TIMESTAMP =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss'Z'").withZone(ZoneOffset.UTC);

    private final Instant startedAt = Instant.now();
    private final String clusterName;
    private final String runId;
    private volatile Instant finishedAt;
    private final Map<String, ShardState> shards = new ConcurrentHashMap<>();

    public DashboardState(Environment environment) {
        this.clusterName = resolveClusterName(environment);
        this.runId = buildRunId(clusterName, startedAt);
    }

    /**
     * The cluster/environment this run targets, taken from the active Spring profile
     * ({@code spring.profiles.active}, e.g. {@code prod}) so there is a single source of truth. Written
     * into every report as both {@code cluster} and {@code environment}, and used as the runId prefix.
     * Falls back to {@code default} when no profile is active.
     */
    private static String resolveClusterName(Environment environment) {
        String[] profiles = environment.getActiveProfiles();
        return (profiles.length > 0 && !profiles[0].isBlank()) ? profiles[0] : "default";
    }

    public String clusterName() {
        return clusterName;
    }

    /**
     * The run identifier, also the report filename stem ({@code backup-report-{runId}.json}) and the id
     * the dashboard fetches by: {@code {clusterName}-{UTC datetime}}, e.g. {@code prod-us-east-2026-07-06T18-30-00Z}.
     */
    private static String buildRunId(String clusterName, Instant startedAt) {
        String cluster = (clusterName == null || clusterName.isBlank()) ? "cluster" : clusterName;
        // Sanitize so the runId is safe as both a filename and a URL path segment.
        String safeCluster = cluster.replaceAll("[^A-Za-z0-9._-]", "-");
        return safeCluster + "-" + RUN_TIMESTAMP.format(startedAt);
    }

    public String runId() {
        return runId;
    }

    public Instant startedAt() {
        return startedAt;
    }

    public Instant finishedAt() {
        return finishedAt;
    }

    public void markRunFinished() {
        finishedAt = Instant.now();
    }

    public void registerPending(String alias, String collection, String shardName, String coreName, String leaderUrl) {
        shards.put(
                key(alias, shardName),
                new ShardState(
                        alias, collection, shardName, coreName, leaderUrl,
                        ShardStatus.PENDING, null, null, null, List.of()));
    }

    /** Begins a new attempt: appends a {@code RUNNING} attempt and moves the shard to {@code RUNNING}. */
    public void startAttempt(String alias, String shardName, int attemptNumber) {
        shards.computeIfPresent(key(alias, shardName), (key, previous) -> {
            Instant now = Instant.now();
            List<ShardAttempt> attempts = new ArrayList<>(previous.attempts());
            attempts.add(new ShardAttempt(attemptNumber, ShardStatus.RUNNING, now, null, null, null));
            Instant shardStartedAt = previous.startedAt() != null ? previous.startedAt() : now;
            return new ShardState(
                    previous.alias(), previous.collection(), previous.shardName(),
                    previous.coreName(), previous.leaderUrl(),
                    ShardStatus.RUNNING, shardStartedAt, null, null, List.copyOf(attempts));
        });
    }

    /** Completes the current (most recent) attempt with its outcome; the shard's overall status is unchanged. */
    public void finishAttempt(String alias, String shardName, ShardStatus attemptStatus, String error) {
        shards.computeIfPresent(key(alias, shardName), (key, previous) -> {
            List<ShardAttempt> attempts = new ArrayList<>(previous.attempts());
            if (!attempts.isEmpty()) {
                ShardAttempt last = attempts.get(attempts.size() - 1);
                Instant finishedAt = Instant.now();
                attempts.set(
                        attempts.size() - 1,
                        new ShardAttempt(
                                last.attempt(),
                                attemptStatus,
                                last.startedAt(),
                                finishedAt,
                                Duration.between(last.startedAt(), finishedAt),
                                error));
            }
            return new ShardState(
                    previous.alias(), previous.collection(), previous.shardName(),
                    previous.coreName(), previous.leaderUrl(),
                    previous.status(), previous.startedAt(), previous.finishedAt(), previous.error(),
                    List.copyOf(attempts));
        });
    }

    /** Sets the shard's terminal overall outcome ({@code SUCCESS}/{@code ERROR}) once all attempts are done. */
    public void markShardResult(String alias, String shardName, ShardStatus status, String error) {
        shards.computeIfPresent(
                key(alias, shardName),
                (key, previous) -> new ShardState(
                        previous.alias(), previous.collection(), previous.shardName(),
                        previous.coreName(), previous.leaderUrl(),
                        status, previous.startedAt(), Instant.now(), error, previous.attempts()));
    }

    public List<ShardState> snapshot() {
        return List.copyOf(shards.values());
    }

    private static String key(String alias, String shardName) {
        return alias + KEY_SEPARATOR + shardName;
    }
}
