/* (C)Team Eclipse 2024 */
package org.example.dashboard;

import java.time.Instant;
import java.util.List;

/**
 * Live in-memory snapshot of a single shard's backup status, as known to the dashboard. {@code status},
 * {@code startedAt}, {@code finishedAt} and {@code error} are the shard's overall state; {@code attempts}
 * holds every individual backup attempt (initial try plus any retries).
 */
public record ShardState(
        String alias,
        String collection,
        String shardName,
        String coreName,
        String leaderUrl,
        ShardStatus status,
        Instant startedAt,
        Instant finishedAt,
        String error,
        List<ShardAttempt> attempts) {}
