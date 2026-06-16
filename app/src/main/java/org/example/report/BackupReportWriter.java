/* (C)Team Eclipse 2024 */
package org.example.report;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.configuration.SolrBackupConfiguration;
import org.springframework.stereotype.Service;

/** Builds the backup summary report from per-shard results and writes it to disk as JSON. */
@Service
@RequiredArgsConstructor
@Slf4j
public class BackupReportWriter {
    /** Filesystem-safe timestamp, e.g. {@code 2026-06-16T05-36-59Z}. */
    private static final DateTimeFormatter FILE_TIMESTAMP =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss'Z'").withZone(ZoneOffset.UTC);

    private final SolrBackupConfiguration solrBackupConfiguration;
    private final ObjectMapper objectMapper;

    public BackupReport buildReport(List<ShardBackupResult> results) {
        int successCount = (int) results.stream()
                .filter(r -> r.outcome() == BackupOutcome.SUCCESS)
                .count();
        int timedOutCount = (int) results.stream()
                .filter(r -> r.outcome() == BackupOutcome.TIMED_OUT)
                .count();
        int erroredCount = (int) results.stream()
                .filter(r -> r.outcome() == BackupOutcome.ERROR)
                .count();

        List<BackupReport.ReplicaRef> timedOutReplicas = projectReplicas(results, BackupOutcome.TIMED_OUT);
        List<BackupReport.ReplicaRef> erroredReplicas = projectReplicas(results, BackupOutcome.ERROR);

        // Group by alias, preserving first-seen order.
        Map<String, List<ShardBackupResult>> byAlias = new LinkedHashMap<>();
        for (ShardBackupResult result : results) {
            byAlias.computeIfAbsent(result.alias(), key -> new java.util.ArrayList<>())
                    .add(result);
        }

        List<BackupReport.AliasReport> aliases = byAlias.entrySet().stream()
                .map(entry -> buildAliasReport(entry.getKey(), entry.getValue()))
                .toList();

        return new BackupReport(
                successCount, timedOutCount, erroredCount, timedOutReplicas, erroredReplicas, aliases);
    }

    public void writeReport(List<ShardBackupResult> results) {
        BackupReport report = buildReport(results);

        try {
            Path directory = Path.of(solrBackupConfiguration.getReportOutputDirectory());
            Files.createDirectories(directory);
            Path reportPath = directory.resolve("backup-report-" + FILE_TIMESTAMP.format(Instant.now()) + ".json");

            objectMapper.writerWithDefaultPrettyPrinter().writeValue(reportPath.toFile(), report);

            log.atInfo()
                    .setMessage("Wrote backup summary report")
                    .addKeyValue("report_path", reportPath.toString())
                    .addKeyValue("replicas_backed_up_successfully", report.replicasBackedUpSuccessfully())
                    .addKeyValue("replicas_timed_out", report.replicasTimedOut())
                    .addKeyValue("replicas_errored", report.replicasErrored())
                    .log();
        } catch (IOException e) {
            // A reporting failure must not fail an otherwise-complete backup.
            log.atError()
                    .setMessage("Failed to write backup summary report")
                    .addKeyValue("error", e.toString())
                    .addKeyValue("report_output_directory", solrBackupConfiguration.getReportOutputDirectory())
                    .log();
        }
    }

    private static List<BackupReport.ReplicaRef> projectReplicas(
            List<ShardBackupResult> results, BackupOutcome outcome) {
        return results.stream()
                .filter(r -> r.outcome() == outcome)
                .map(r -> new BackupReport.ReplicaRef(
                        r.alias(), r.collection(), r.shardName(), r.coreName(), r.leaderUrl()))
                .toList();
    }

    private static BackupReport.AliasReport buildAliasReport(String alias, List<ShardBackupResult> shards) {
        String collection = shards.stream()
                .map(ShardBackupResult::collection)
                .filter(value -> value != null)
                .findFirst()
                .orElse(null);

        Instant startedAt = shards.stream()
                .map(ShardBackupResult::startedAt)
                .min(Comparator.naturalOrder())
                .orElse(null);
        Instant finishedAt = shards.stream()
                .map(ShardBackupResult::finishedAt)
                .max(Comparator.naturalOrder())
                .orElse(null);

        double averageShardBackupSeconds = shards.stream()
                .mapToDouble(shard ->
                        Duration.between(shard.startedAt(), shard.finishedAt()).toMillis() / 1000.0)
                .average()
                .orElse(0.0);

        return new BackupReport.AliasReport(alias, collection, startedAt, finishedAt, averageShardBackupSeconds);
    }
}
