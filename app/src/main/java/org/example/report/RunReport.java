/* (C)Team Eclipse 2024 */
package org.example.report;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.example.dashboard.ShardStatus;

/**
 * Live, run-scoped backup report serialized to JSON for the standalone dashboard. Unlike the old
 * aggregate summary, this captures every shard's lifecycle status (including {@code PENDING}/
 * {@code RUNNING}) so the file can be rewritten repeatedly during a run and read live.
 */
public record RunReport(
        String runId,
        String cluster,
        String environment,
        RunStatus status,
        Instant startedAt,
        Instant finishedAt,
        int parallelBackups,
        Counts counts,
        List<CollectionReport> collections) {

    /** Shard counts by lifecycle status, plus the total. */
    public record Counts(int pending, int running, int success, int errored, int total) {}

    /** One alias/collection and the shards being backed up under it. */
    public record CollectionReport(String alias, String collection, List<ShardReport> shards) {}

    /** Per-shard live status row, including every backup attempt (the initial try plus any retries). */
    public record ShardReport(
            String shardName,
            String coreName,
            String leaderUrl,
            ShardStatus status,
            Instant startedAt,
            Instant finishedAt,
            String error,
            List<Attempt> attempts) {

        /**
         * One backup attempt for the shard, with its own start/finish timing and outcome. {@code duration}
         * is {@code null} while the attempt is still running.
         */
        public record Attempt(
                int attempt,
                ShardStatus status,
                Instant startedAt,
                Instant finishedAt,
                Duration duration,
                String error) {}
    }
}
