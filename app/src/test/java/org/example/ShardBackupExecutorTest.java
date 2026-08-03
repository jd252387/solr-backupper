/* (C)Team Eclipse 2024 */
package org.example;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.solr.common.cloud.Replica;
import org.example.configuration.SolrBackupConfiguration;
import org.example.dashboard.DashboardState;
import org.example.dashboard.ShardAttempt;
import org.example.dashboard.ShardState;
import org.example.dashboard.ShardStatus;
import org.junit.Test;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.web.reactive.function.client.WebClient;

public class ShardBackupExecutorTest {

    private static final String ALIAS = "Books";
    private static final String SHARD = "s1";

    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
    private final DashboardState state = new DashboardState(new StandardEnvironment());
    private final ShardBackupExecutor executor = new ShardBackupExecutor(null, null, mapper, state);

    public ShardBackupExecutorTest() {
        state.registerPending(ALIAS, "coll1", SHARD, "core1", "http://host:8983/solr/");
        state.startAttempt(ALIAS, SHARD, 1);
    }

    private JsonNode details(String backupBodyJson) throws Exception {
        return mapper.readTree("{\"details\":{\"backup\":" + backupBodyJson + "}}");
    }

    private boolean evaluate(JsonNode response) {
        return executor.isBackupFinished(
                response, new AtomicReference<>(), ALIAS, SHARD, "core1", "http://host:8983/solr/", "coll1");
    }

    /** The current attempt of the one shard this test registers. */
    private ShardAttempt attempt() {
        return state.snapshot().get(0).attempts().get(0);
    }

    @Test
    public void waitingForCommit_keepsPolling() throws Exception {
        // Published before the index commit resolves, so it carries no file counts.
        assertFalse(evaluate(details("{\"startTime\":\"2026-07-14T10:00:05Z\",\"status\":\"waiting for commit\"}")));
        assertNull(attempt().fileCount());
        assertNull(attempt().finishedFileCount());
    }

    @Test
    public void running_keepsPollingAndRecordsProgress() throws Exception {
        assertFalse(evaluate(details(
                "{\"startTime\":\"2026-07-14T10:00:05Z\",\"status\":\"running\",\"fileCount\":500,\"finishedFileCount\":137}")));
        assertEquals(Integer.valueOf(500), attempt().fileCount());
        assertEquals(Integer.valueOf(137), attempt().finishedFileCount());
    }

    @Test
    public void success_isFinished() throws Exception {
        assertTrue(evaluate(details("{\"startTime\":\"2026-07-14T10:00:05Z\",\"status\":\"success\",\"fileCount\":500}")));
    }

    @Test
    public void backupFailed_throws() throws Exception {
        try {
            evaluate(details("{\"exception\":\"disk full\"}"));
            fail("expected our failed backup to throw");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("disk full"));
        }
    }

    @Test
    public void unknownStatus_throws() throws Exception {
        try {
            evaluate(details("{\"startTime\":\"2026-07-14T10:00:05Z\",\"status\":\"failed\"}"));
            fail("expected an unrecognised status to throw");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Backup status was unsuccessful"));
        }
    }

    @Test
    public void noBackupNode_throws() throws Exception {
        try {
            evaluate(mapper.readTree("{\"details\":{}}"));
            fail("expected core-stopped to throw");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Core stopped updating"));
        }
    }

    @Test
    public void isBackupInProgress_detectsBothInProgressStatuses() throws Exception {
        assertTrue(executor.isBackupInProgress(details("{\"status\":\"waiting for commit\"}")));
        assertTrue(executor.isBackupInProgress(details("{\"status\":\"running\",\"fileCount\":3}")));
        assertFalse(
                executor.isBackupInProgress(details("{\"startTime\":\"2026-07-14T09:00:00Z\",\"status\":\"success\"}")));
        assertFalse(executor.isBackupInProgress(mapper.readTree("{\"details\":{}}")));
    }

    @Test
    public void everyAttemptResolvesTheLeaderAgain() throws Exception {
        SolrBackupConfiguration config = new SolrBackupConfiguration();
        config.setBackupsMount(Files.createTempDirectory("backup-mount").toString());
        config.setRetries(2);
        config.setRetryDelay(Duration.ofMillis(1));
        config.setDeleteFailedBackupDelay(Duration.ofMillis(1));

        DashboardState retryState = new DashboardState(new StandardEnvironment());
        retryState.registerPending(ALIAS, "coll1", SHARD, "core1", "http://host:8983/solr/");
        AtomicInteger resolutions = new AtomicInteger();

        new ShardBackupExecutor(config, null, mapper, retryState)
                .backupShard(ALIAS, "coll1", SHARD, () -> {
                    resolutions.incrementAndGet();
                    throw new IllegalStateException("no leader yet");
                })
                .block();

        // Initial try + 2 retries, each resolving the leader itself rather than reusing the first one.
        assertEquals(3, resolutions.get());
        ShardState shard = retryState.snapshot().get(0);
        assertEquals(3, shard.attempts().size());
        assertEquals(ShardStatus.ERROR, shard.status());
        assertTrue(shard.error(), shard.error().contains("no leader yet"));
    }

    /**
     * Drives a real attempt against a stub Solr (the JDK's own HTTP server) that reports a failed backup, and
     * checks the two things the attempt does around that failure: the status poll is tried three times before
     * the attempt gives up, and the partial snapshot is deleted afterwards.
     */
    @Test
    public void failedPoll_isRetriedThreeTimesThenTheSnapshotIsDeleted() throws Exception {
        Path mount = Files.createTempDirectory("backup-mount");
        Path snapshot = Files.createDirectories(mount.resolve(ALIAS).resolve(SHARD).resolve("snapshot.1"));
        Files.writeString(snapshot.resolve("segments_1"), "partial");

        AtomicInteger detailsCalls = new AtomicInteger();
        HttpServer solr = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        solr.createContext("/solr/core1/replication", exchange -> {
            String body;
            if (exchange.getRequestURI().getQuery().contains("command=details")) {
                // Idle on the pre-flight check, then a failed backup on every poll after the trigger.
                body = detailsCalls.incrementAndGet() == 1
                        ? "{\"details\":{}}"
                        : "{\"details\":{\"backup\":{\"directoryName\":\"snapshot.1\",\"exception\":\"disk full\"}}}";
            } else {
                body = "{\"status\":\"OK\"}";
            }
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        });
        solr.start();

        try {
            String baseUrl = "http://127.0.0.1:" + solr.getAddress().getPort() + "/solr";
            SolrBackupConfiguration config = new SolrBackupConfiguration();
            config.setBackupsMount(mount.toString());
            config.setRetries(0); // one attempt, so the poll retries are the only ones counted
            config.setRetryDelay(Duration.ofMillis(1));
            config.setStatusEvery(Duration.ofMillis(1));
            config.setDeleteFailedBackupDelay(Duration.ofMillis(1));

            DashboardState pollState = new DashboardState(new StandardEnvironment());
            pollState.registerPending(ALIAS, "coll1", SHARD, "core1", baseUrl + "/core1/");
            Replica leader = new Replica(
                    "core_node1",
                    Map.of(
                            "core", "core1",
                            "base_url", baseUrl,
                            "node_name", "127.0.0.1:" + solr.getAddress().getPort() + "_solr",
                            "type", "NRT",
                            "state", "active"),
                    "coll1",
                    SHARD);

            new ShardBackupExecutor(config, WebClient.builder().build(), mapper, pollState)
                    .backupShard(ALIAS, "coll1", SHARD, () -> leader)
                    .block();

            // One idle pre-flight check, then the poll tried three times before the attempt failed.
            assertEquals(4, detailsCalls.get());
            ShardState shard = pollState.snapshot().get(0);
            assertEquals(ShardStatus.ERROR, shard.status());
            // The original failure, not Reactor's "Retries exhausted" wrapper.
            assertTrue(shard.error(), shard.error().contains("disk full"));
            assertFalse("the failed attempt's snapshot should have been deleted", Files.exists(snapshot));
        } finally {
            solr.stop(0);
        }
    }

    @Test
    public void succeededAttempt_reportsEveryFileCopied() throws Exception {
        // Polling usually misses Solr's last "running" report, so a success completes the count itself.
        evaluate(details(
                "{\"startTime\":\"2026-07-14T10:00:05Z\",\"status\":\"running\",\"fileCount\":500,\"finishedFileCount\":300}"));
        state.finishAttempt(ALIAS, SHARD, ShardStatus.SUCCESS, null);

        assertEquals(Integer.valueOf(500), attempt().fileCount());
        assertEquals(Integer.valueOf(500), attempt().finishedFileCount());
    }
}
