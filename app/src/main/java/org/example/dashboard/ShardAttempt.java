/* (C)Team Eclipse 2024 */
package org.example.dashboard;

import java.time.Duration;
import java.time.Instant;

/**
 * One backup attempt for a shard — the initial try or a retry — with its own start/finish timing and
 * outcome. A shard accumulates one of these per attempt so the dashboard can show every try.
 * {@code duration} is {@code null} while the attempt is still running and set to the elapsed time
 * ({@code finishedAt - startedAt}) once it succeeds or fails.
 *
 * <p>{@code coreName}/{@code leaderUrl} are the leader replica <b>this</b> attempt resolved. They live on the
 * attempt rather than only on the shard because the leader can move between attempts — that is usually why a
 * retry happens — and a shard-level field would report only the last one. Both stay {@code null} for an
 * attempt that failed before a leader was resolved (no elected leader), which is itself the useful signal.
 *
 * <p>{@code fileCount}/{@code finishedFileCount} are the file-copy progress Solr reports while the backup
 * runs ({@code status=running} on {@code command=details}). Both are {@code null} until the core resolves
 * its index commit and reports a file list, so a failed attempt keeps the progress it died at.
 */
public record ShardAttempt(
        int attempt,
        ShardStatus status,
        Instant startedAt,
        Instant finishedAt,
        Duration duration,
        String error,
        String coreName,
        String leaderUrl,
        Integer fileCount,
        Integer finishedFileCount) {}
