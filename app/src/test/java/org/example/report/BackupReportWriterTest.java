/* (C)Team Eclipse 2024 */
package org.example.report;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.example.configuration.SolrBackupConfiguration;
import org.junit.Test;

public class BackupReportWriterTest {

    private BackupReportWriter newWriter(String reportOutputDirectory) {
        SolrBackupConfiguration config = new SolrBackupConfiguration();
        config.setReportOutputDirectory(reportOutputDirectory);
        return new BackupReportWriter(config, new ObjectMapper().findAndRegisterModules());
    }

    private ShardBackupResult result(
            String alias,
            String collection,
            String shard,
            BackupOutcome outcome,
            long startEpochSeconds,
            long endEpochSeconds) {
        return new ShardBackupResult(
                alias,
                collection,
                shard,
                collection + "_" + shard + "_replica_n1",
                "http://host:8983/solr/" + collection + "_" + shard + "_replica_n1/",
                outcome,
                Instant.ofEpochSecond(startEpochSeconds),
                Instant.ofEpochSecond(endEpochSeconds));
    }

    @Test
    public void buildsReportWithCountsListsAndPerAliasAverages() {
        BackupReportWriter writer = newWriter("/tmp/unused");

        List<ShardBackupResult> results = List.of(
                result("Books", "books_v2", "shard1", BackupOutcome.SUCCESS, 0, 10),
                result("Books", "books_v2", "shard2", BackupOutcome.SUCCESS, 0, 20),
                result("Products", "products_v3", "shard1", BackupOutcome.TIMED_OUT, 0, 30),
                result("Products", "products_v3", "shard2", BackupOutcome.ERROR, 0, 40));

        BackupReport report = writer.buildReport(results);

        assertEquals(2, report.replicasBackedUpSuccessfully());
        assertEquals(1, report.replicasTimedOut());
        assertEquals(1, report.replicasErrored());

        assertEquals(1, report.timedOutReplicas().size());
        assertEquals("Products", report.timedOutReplicas().get(0).alias());
        assertEquals("shard1", report.timedOutReplicas().get(0).shardName());

        assertEquals(1, report.erroredReplicas().size());
        assertEquals("shard2", report.erroredReplicas().get(0).shardName());

        assertEquals(2, report.aliases().size());
        BackupReport.AliasReport books = report.aliases().stream()
                .filter(alias -> alias.alias().equals("Books"))
                .findFirst()
                .orElseThrow();
        assertEquals("books_v2", books.collection());
        assertEquals(Instant.ofEpochSecond(0), books.backupStartedAt());
        assertEquals(Instant.ofEpochSecond(20), books.backupFinishedAt());
        // (10s + 20s) / 2 shards
        assertEquals(15.0, books.averageShardBackupSeconds(), 0.0001);
    }

    @Test
    public void writesTimestampedJsonFile() throws Exception {
        Path directory = Files.createTempDirectory("backup-report-test");
        BackupReportWriter writer = newWriter(directory.toString());

        writer.writeReport(List.of(result("Books", "books_v2", "shard1", BackupOutcome.SUCCESS, 0, 10)));

        List<Path> files;
        try (var stream = Files.list(directory)) {
            files = stream.toList();
        }
        assertEquals(1, files.size());

        String fileName = files.get(0).getFileName().toString();
        assertTrue(fileName, fileName.startsWith("backup-report-") && fileName.endsWith(".json"));

        JsonNode node = new ObjectMapper().readTree(files.get(0).toFile());
        assertEquals(1, node.get("replicasBackedUpSuccessfully").asInt());
        assertEquals(0, node.get("replicasTimedOut").asInt());
        assertEquals(1, node.get("aliases").size());
        assertEquals("books_v2", node.get("aliases").get(0).get("collection").asText());
    }
}
