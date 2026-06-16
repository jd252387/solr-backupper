/* (C)Team Eclipse 2024 */
package org.example.report;

import java.time.Instant;
import java.util.List;

/** The summary report serialized to JSON at the end of a backup run. */
public record BackupReport(
        int replicasBackedUpSuccessfully,
        int replicasTimedOut,
        int replicasErrored,
        List<ReplicaRef> timedOutReplicas,
        List<ReplicaRef> erroredReplicas,
        List<AliasReport> aliases) {

    /** Identifying details for a single shard's leader replica. */
    public record ReplicaRef(
            String alias, String collection, String shardName, String coreName, String leaderUrl) {}

    /** Per-alias backup summary. */
    public record AliasReport(
            String alias,
            String collection,
            Instant backupStartedAt,
            Instant backupFinishedAt,
            double averageShardBackupSeconds) {}
}
