/* (C)Team Eclipse 2024 */
package org.example;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

public class ShardBackupExecutorTest {

    private static final LocalDateTime TRIGGER = LocalDateTime.parse("2026-07-14T10:00:00");

    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
    private final ShardBackupExecutor executor = new ShardBackupExecutor(null, null, mapper, null);

    private JsonNode details(String backupBodyJson) throws Exception {
        return mapper.readTree("{\"details\":{\"backup\":" + backupBodyJson + "}}");
    }

    private boolean evaluate(JsonNode response) {
        return executor.isBackupFinished(
                response, new AtomicReference<>(), TRIGGER, "s1", "core1", "http://host:8983/solr/", "coll1");
    }

    @Test
    public void previousCompletedBackup_keepsPolling() throws Exception {
        // A backup that started before we triggered is the previous one, even if status=success.
        assertFalse(evaluate(details("{\"startTime\":\"2026-07-14T09:00:00\",\"status\":\"success\"}")));
    }

    @Test
    public void ourBackupInProgress_keepsPolling() throws Exception {
        assertFalse(evaluate(details("{\"startTime\":\"2026-07-14T10:00:05\",\"status\":\"In Progress\"}")));
    }

    @Test
    public void ourBackupSucceeded_isFinished() throws Exception {
        assertTrue(evaluate(details("{\"startTime\":\"2026-07-14T10:00:05\",\"status\":\"success\"}")));
    }

    @Test
    public void ourBackupSucceeded_withZoneSuffix_isFinished() throws Exception {
        // Solr may report an ISO timestamp with a trailing offset; parsing must still work.
        assertTrue(evaluate(details("{\"startTime\":\"2026-07-14T10:00:05Z\",\"status\":\"success\"}")));
    }

    @Test
    public void ourBackupFailed_throws() throws Exception {
        try {
            evaluate(details("{\"startTime\":\"2026-07-14T10:00:05\",\"status\":\"failed\",\"exception\":\"disk full\"}"));
            fail("expected our failed backup to throw");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("disk full"));
        }
    }

    @Test
    public void foreignBackupInProgress_throws() throws Exception {
        // A backup started before our trigger that is still In Progress means one is already running.
        try {
            evaluate(details("{\"startTime\":\"2026-07-14T09:59:00\",\"status\":\"In Progress\"}"));
            fail("expected already-in-progress to throw");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("already in progress"));
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
    public void isBackupInProgress_detectsInProgressStatus() throws Exception {
        assertTrue(executor.isBackupInProgress(details("{\"status\":\"In Progress\"}")));
        assertFalse(
                executor.isBackupInProgress(details("{\"startTime\":\"2026-07-14T09:00:00\",\"status\":\"success\"}")));
        assertFalse(executor.isBackupInProgress(mapper.readTree("{\"details\":{}}")));
    }
}
