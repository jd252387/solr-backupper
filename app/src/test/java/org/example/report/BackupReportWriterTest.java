/* (C)Team Eclipse 2024 */
package org.example.report;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.example.configuration.SolrBackupConfiguration;
import org.example.dashboard.DashboardState;
import org.example.dashboard.ShardStatus;
import org.junit.Test;
import org.springframework.core.env.StandardEnvironment;

public class BackupReportWriterTest {

    private SolrBackupConfiguration config(String reportOutputDirectory) {
        SolrBackupConfiguration config = new SolrBackupConfiguration();
        config.setReportOutputDirectory(reportOutputDirectory);
        config.setParallelBackups(2);
        return config;
    }

    // Cluster/environment (and the runId prefix) now come from the active Spring profile.
    private DashboardState dashboardState() {
        StandardEnvironment environment = new StandardEnvironment();
        environment.setActiveProfiles("test-cluster");
        return new DashboardState(environment);
    }

    private BackupReportWriter writer(SolrBackupConfiguration config, DashboardState state) {
        return new BackupReportWriter(config, new ObjectMapper().findAndRegisterModules(), state);
    }

    private void register(DashboardState state, String alias, String collection, String shard) {
        String core = collection + "_" + shard + "_replica_n1";
        state.registerPending(alias, collection, shard, core, "http://host:8983/solr/" + core + "/");
    }

    private void succeed(DashboardState state, String alias, String shard) {
        state.startAttempt(alias, shard, 1);
        state.finishAttempt(alias, shard, ShardStatus.SUCCESS, null);
        state.markShardResult(alias, shard, ShardStatus.SUCCESS, null);
    }

    private void fail(DashboardState state, String alias, String shard, String error) {
        state.startAttempt(alias, shard, 1);
        state.finishAttempt(alias, shard, ShardStatus.ERROR, error);
        state.markShardResult(alias, shard, ShardStatus.ERROR, error);
    }

    @Test
    public void buildsRunReportWithCountsGroupingAndDerivedStatus() {
        SolrBackupConfiguration config = config("/tmp/unused");
        DashboardState state = dashboardState();

        register(state, "Books", "books_v2", "shard1");
        succeed(state, "Books", "shard1");
        register(state, "Books", "books_v2", "shard2");
        succeed(state, "Books", "shard2");
        register(state, "Products", "products_v3", "shard1");
        fail(state, "Products", "shard1", "core stopped updating on backup");
        register(state, "Products", "products_v3", "shard2");
        fail(state, "Products", "shard2", "boom");
        state.markRunFinished();

        RunReport report = writer(config, state).buildReport();

        assertEquals("test-cluster", report.cluster());
        assertEquals("test-cluster", report.environment());
        assertEquals(2, report.parallelBackups());
        // runId is {clusterName}-{datetime}.
        assertTrue(report.runId(), report.runId().startsWith("test-cluster-"));
        // Any errored shard makes the whole (finished) run ERROR.
        assertEquals(RunStatus.ERROR, report.status());

        assertEquals(4, report.counts().total());
        assertEquals(2, report.counts().success());
        assertEquals(2, report.counts().errored());
        assertEquals(0, report.counts().pending());
        assertEquals(0, report.counts().running());

        // Grouped by alias/collection, aliases sorted alphabetically (Books before Products).
        assertEquals(2, report.collections().size());
        RunReport.CollectionReport books = report.collections().get(0);
        assertEquals("Books", books.alias());
        assertEquals("books_v2", books.collection());
        assertEquals(2, books.shards().size());
        assertEquals("shard1", books.shards().get(0).shardName());
        assertEquals(ShardStatus.SUCCESS, books.shards().get(0).status());
    }

    @Test
    public void activeRunReportsActiveStatusWhileShardsPending() {
        SolrBackupConfiguration config = config("/tmp/unused");
        DashboardState state = dashboardState();
        register(state, "Books", "books_v2", "shard1");
        state.startAttempt("Books", "shard1", 1);
        register(state, "Books", "books_v2", "shard2"); // still PENDING, run not finished

        RunReport report = writer(config, state).buildReport();

        assertEquals(RunStatus.ACTIVE, report.status());
        assertEquals(1, report.counts().running());
        assertEquals(1, report.counts().pending());
    }

    @Test
    public void writesStableAtomicJsonFileRewrittenInPlace() throws Exception {
        Path directory = Files.createTempDirectory("run-report-test");
        SolrBackupConfiguration config = config(directory.toString());
        DashboardState state = dashboardState();
        register(state, "Books", "books_v2", "shard1");
        succeed(state, "Books", "shard1");
        state.markRunFinished();

        BackupReportWriter writer = writer(config, state);
        writer.writeReport();
        writer.writeReport(); // second write must replace the same file, not create a new one

        List<Path> jsonFiles;
        try (var stream = Files.list(directory)) {
            jsonFiles = stream.filter(p -> p.getFileName().toString().endsWith(".json")).toList();
        }
        assertEquals(1, jsonFiles.size());

        String fileName = jsonFiles.get(0).getFileName().toString();
        assertTrue(fileName, fileName.startsWith("backup-report-test-cluster-") && fileName.endsWith(".json"));

        // No leftover temp files.
        try (var stream = Files.list(directory)) {
            assertFalse(stream.anyMatch(p -> p.getFileName().toString().endsWith(".tmp")));
        }

        JsonNode node = new ObjectMapper().readTree(jsonFiles.get(0).toFile());
        assertEquals("test-cluster", node.get("cluster").asText());
        assertEquals("SUCCESS", node.get("status").asText());
        assertEquals(1, node.get("counts").get("success").asInt());
        assertEquals("books_v2", node.get("collections").get(0).get("collection").asText());
        assertTrue(node.get("runId").asText().startsWith("test-cluster-"));
    }

    @Test
    public void shardThatSucceedsOnRetryIsSuccessWithEveryAttemptRecorded() {
        SolrBackupConfiguration config = config("/tmp/unused");
        DashboardState state = dashboardState();

        // A shard that fails its first attempt and succeeds on the retry.
        register(state, "Books", "books_v2", "shard1");
        state.startAttempt("Books", "shard1", 1);
        state.finishAttempt("Books", "shard1", ShardStatus.ERROR, "boom");
        state.startAttempt("Books", "shard1", 2);
        state.finishAttempt("Books", "shard1", ShardStatus.SUCCESS, null);
        state.markShardResult("Books", "shard1", ShardStatus.SUCCESS, null);
        state.markRunFinished();

        RunReport report = writer(config, state).buildReport();

        // Succeeding on retry makes the shard SUCCESS and the finished run SUCCESS.
        assertEquals(RunStatus.SUCCESS, report.status());
        assertEquals(1, report.counts().success());
        assertEquals(0, report.counts().errored());

        RunReport.ShardReport shard = report.collections().get(0).shards().get(0);
        assertEquals(ShardStatus.SUCCESS, shard.status());
        // Both attempts are recorded: first ERROR, then SUCCESS.
        assertEquals(2, shard.attempts().size());
        assertEquals(1, shard.attempts().get(0).attempt());
        assertEquals(ShardStatus.ERROR, shard.attempts().get(0).status());
        assertEquals("boom", shard.attempts().get(0).error());
        assertEquals(ShardStatus.SUCCESS, shard.attempts().get(1).status());
    }
}
