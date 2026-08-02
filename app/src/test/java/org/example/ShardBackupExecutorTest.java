/* (C)Team Eclipse 2024 */
package org.example;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.example.configuration.SolrBackupConfiguration;
import org.example.dashboard.DashboardState;
import org.example.dashboard.ShardAttempt;
import org.example.dashboard.ShardState;
import org.example.dashboard.ShardStatus;
import org.junit.Test;
import org.springframework.core.env.StandardEnvironment;

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
