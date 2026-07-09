/* (C)Team Eclipse 2024 */
package org.example.dashboard;

import java.time.Duration;
import java.time.Instant;

/**
 * One backup attempt for a shard — the initial try or a retry — with its own start/finish timing and
 * outcome. A shard accumulates one of these per attempt so the dashboard can show every try.
 * {@code duration} is {@code null} while the attempt is still running and set to the elapsed time
 * ({@code finishedAt - startedAt}) once it succeeds or fails.
 */
public record ShardAttempt(
        int attempt, ShardStatus status, Instant startedAt, Instant finishedAt, Duration duration, String error) {}
