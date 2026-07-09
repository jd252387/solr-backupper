/* (C)Team Eclipse 2024 */
package org.example.report;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.configuration.SolrBackupConfiguration;
import org.example.dashboard.DashboardState;
import org.example.dashboard.ShardState;
import org.example.dashboard.ShardStatus;
import org.springframework.stereotype.Service;

/**
 * Builds the live {@link RunReport} from {@link DashboardState} and writes it to disk as JSON. The
 * report is rewritten repeatedly during a run (see {@link org.example.Runner}) to a stable per-run
 * filename, using an atomic temp-file-then-rename so a concurrent reader (the dashboard) never
 * observes a half-written file.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class BackupReportWriter {
    /** Splits a shard name into alternating digit/non-digit runs, e.g. "shard10" -> ["shard", "10"]. */
    private static final Pattern NAME_CHUNK = Pattern.compile("\\d+|\\D+");

    /** Separates the grouping key parts; NUL never appears in an alias or collection name. */
    private static final char GROUP_SEPARATOR = '\0';

    private final SolrBackupConfiguration solrBackupConfiguration;
    private final ObjectMapper objectMapper;
    private final DashboardState dashboardState;

    /**
     * Builds the current run report from the live dashboard state. Safe to call repeatedly during a
     * run; tolerates shards that are still {@code PENDING}/{@code RUNNING} (null {@code finishedAt}).
     */
    public RunReport buildReport() {
        List<ShardState> shards = dashboardState.snapshot();

        // Group by alias+collection. Sort first (alias, then natural shard order) so both the
        // collection order and the shard order within each collection are stable across writes.
        Map<String, List<ShardState>> byCollection = new LinkedHashMap<>();
        shards.stream()
                .sorted(Comparator.comparing(ShardState::alias)
                        .thenComparing(ShardState::shardName, BackupReportWriter::compareNatural))
                .forEach(shard -> byCollection
                        .computeIfAbsent(
                                shard.alias() + GROUP_SEPARATOR + shard.collection(), key -> new ArrayList<>())
                        .add(shard));

        List<RunReport.CollectionReport> collections = byCollection.values().stream()
                .map(group -> new RunReport.CollectionReport(
                        group.get(0).alias(),
                        group.get(0).collection(),
                        group.stream()
                                .map(shard -> new RunReport.ShardReport(
                                        shard.shardName(),
                                        shard.coreName(),
                                        shard.leaderUrl(),
                                        shard.status(),
                                        shard.startedAt(),
                                        shard.finishedAt(),
                                        shard.error(),
                                        shard.attempts().stream()
                                                .map(attempt -> new RunReport.ShardReport.Attempt(
                                                        attempt.attempt(),
                                                        attempt.status(),
                                                        attempt.startedAt(),
                                                        attempt.finishedAt(),
                                                        attempt.duration(),
                                                        attempt.error()))
                                                .toList()))
                                .toList()))
                .toList();

        // Cluster and environment are the same value now: the active Spring profile (see DashboardState).
        String cluster = dashboardState.clusterName();
        return new RunReport(
                dashboardState.runId(),
                cluster,
                cluster,
                deriveStatus(shards, dashboardState.finishedAt()),
                dashboardState.startedAt(),
                dashboardState.finishedAt(),
                solrBackupConfiguration.getParallelBackups(),
                countByStatus(shards),
                collections);
    }

    /**
     * Serializes the current run report to {@code backup-report-{runId}.json}, replacing any prior
     * copy atomically. A write failure is logged, not thrown — reporting must never fail an
     * otherwise-successful backup run.
     */
    public void writeReport() {
        RunReport report = buildReport();

        try {
            Path directory = Path.of(solrBackupConfiguration.getReportOutputDirectory());
            Files.createDirectories(directory);
            Path reportPath = directory.resolve("backup-report-" + report.runId() + ".json");

            Path tempPath = Files.createTempFile(directory, "backup-report-" + report.runId() + "-", ".json.tmp");
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(tempPath.toFile(), report);
            try {
                Files.move(
                        tempPath, reportPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                // Some filesystems (e.g. certain network mounts) don't support atomic moves; fall back
                // to a plain replace. Still far better than writing straight to the final path.
                Files.move(tempPath, reportPath, StandardCopyOption.REPLACE_EXISTING);
            }

            log.atDebug()
                    .setMessage("Wrote live backup report")
                    .addKeyValue("report_path", reportPath.toString())
                    .addKeyValue("run_status", report.status())
                    .addKeyValue("shards_total", report.counts().total())
                    .log();
        } catch (IOException e) {
            log.atError()
                    .setMessage("Failed to write backup report")
                    .addKeyValue("error", e.toString())
                    .addKeyValue("report_output_directory", solrBackupConfiguration.getReportOutputDirectory())
                    .log();
        }
    }

    private static RunReport.Counts countByStatus(List<ShardState> shards) {
        int pending = 0;
        int running = 0;
        int success = 0;
        int errored = 0;
        for (ShardState shard : shards) {
            switch (shard.status()) {
                case PENDING -> pending++;
                case RUNNING -> running++;
                case SUCCESS -> success++;
                case ERROR -> errored++;
            }
        }
        return new RunReport.Counts(pending, running, success, errored, shards.size());
    }

    /**
     * A run is {@code ACTIVE} until it is marked finished; once finished it is {@code ERROR} if any shard
     * errored, otherwise {@code SUCCESS}. Shards no longer time out — a stalled or interrupted backup
     * surfaces as an {@code ERROR} shard (see {@link org.example.ShardBackupExecutor}).
     */
    private static RunStatus deriveStatus(List<ShardState> shards, java.time.Instant runFinishedAt) {
        if (runFinishedAt == null) {
            return RunStatus.ACTIVE;
        }
        boolean anyErrored = shards.stream().anyMatch(shard -> shard.status() == ShardStatus.ERROR);
        return anyErrored ? RunStatus.ERROR : RunStatus.SUCCESS;
    }

    /** Natural-order comparison so "shard2" sorts before "shard10" instead of after it. */
    private static int compareNatural(String a, String b) {
        Matcher matcherA = NAME_CHUNK.matcher(a);
        Matcher matcherB = NAME_CHUNK.matcher(b);
        while (matcherA.find() && matcherB.find()) {
            String chunkA = matcherA.group();
            String chunkB = matcherB.group();
            int comparison = Character.isDigit(chunkA.charAt(0)) && Character.isDigit(chunkB.charAt(0))
                    ? new BigInteger(chunkA).compareTo(new BigInteger(chunkB))
                    : chunkA.compareTo(chunkB);
            if (comparison != 0) {
                return comparison;
            }
        }
        return a.length() - b.length();
    }
}
