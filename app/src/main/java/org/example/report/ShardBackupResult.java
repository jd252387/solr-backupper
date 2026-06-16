/* (C)Team Eclipse 2024 */
package org.example.report;

import java.time.Instant;

/** The outcome and timing of a single shard (leader replica) backup attempt. */
public record ShardBackupResult(
        String alias,
        String collection,
        String shardName,
        String coreName,
        String leaderUrl,
        BackupOutcome outcome,
        Instant startedAt,
        Instant finishedAt) {}
