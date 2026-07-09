/* (C)Team Eclipse 2024 */
package org.example.dashboard;

/** Live status of a shard backup, tracked for the dashboard while a run is in progress. */
public enum ShardStatus {
    PENDING,
    RUNNING,
    SUCCESS,
    ERROR
}
