/* (C)Team Eclipse 2024 */
package org.example.report;

/**
 * Overall status of a backup run, derived from its shards' live statuses: a run is {@link #ACTIVE}
 * while it is in progress, then settles into {@link #SUCCESS} (every shard backed up) or
 * {@link #ERROR} (at least one shard errored or timed out — a timeout counts as a failure, there is
 * no separate partial state).
 */
public enum RunStatus {
    ACTIVE,
    SUCCESS,
    ERROR
}
