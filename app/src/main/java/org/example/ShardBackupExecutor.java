/* (C)Team Eclipse 2024 */
package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.utils.URIBuilder;
import org.apache.solr.common.cloud.Replica;
import org.example.configuration.SolrBackupConfiguration;
import org.example.dashboard.DashboardState;
import org.example.dashboard.ShardStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

/**
 * Executes a single shard's native Solr replication backup: trigger, poll until finished, and record the
 * outcome into {@link DashboardState}. Each attempt is retried up to {@code solr.backup.retries} times; a
 * shard that succeeds on any attempt is {@code SUCCESS}, and every failed attempt has its partial snapshot
 * deleted from disk so failed replications don't linger on the mount. Extracted from {@link Runner} (which
 * drives the full-run pipeline) so it works from plain identifying strings, plus a supplier that resolves
 * the shard's leader afresh on every attempt — a retry typically follows the failure that moved the leader.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ShardBackupExecutor {
    private final SolrBackupConfiguration solrBackupConfiguration;
    private final WebClient webClient;
    private final ObjectMapper objectMapper;
    private final DashboardState dashboardState;

    /** The status Solr reports once a backup has finished successfully. */
    private static final String SUCCESS_STATUS = "success";

    /**
     * The statuses Solr reports while a backup is still running (see {@code SnapShooter}):
     * {@code waiting for commit} before the index commit — and with it the file list — is resolved, then
     * {@code running} after every copied file.
     */
    private static final Set<String> IN_PROGRESS_STATUSES = Set.of("waiting for commit", "running");

    private static boolean isInProgress(String status) {
        return status != null && IN_PROGRESS_STATUSES.contains(status.toLowerCase(Locale.ROOT));
    }

    /** Returns {@code details.backup} from a {@code command=details} response, or {@code null}. */
    private static JsonNode backupNode(JsonNode responseNode) {
        return Optional.ofNullable(responseNode.get("details"))
                .map(detailsNode -> detailsNode.get("backup"))
                .filter(node -> !node.isNull())
                .orElse(null);
    }

    private static String exceptionSuffix(JsonNode backupNode) {
        return Optional.ofNullable(backupNode.get("exception"))
                .map(JsonNode::textValue)
                .map(message -> " - " + message)
                .orElse("");
    }

    /** True when the core's {@code command=details} reports a backup that is still running. */
    boolean isBackupInProgress(JsonNode responseNode) {
        JsonNode backupNode = backupNode(responseNode);
        if (backupNode == null) {
            return false;
        }
        return isInProgress(Optional.ofNullable(backupNode.get("status"))
                .map(JsonNode::textValue)
                .orElse(null));
    }

    /**
     * Evaluates one {@code command=details} poll of the backup we triggered:
     *
     * <ul>
     *   <li>no backup reported at all → throws (Solr publishes a status before {@code command=backup} even
     *       returns, so the key disappearing means the core stopped reporting on ours);
     *   <li>{@code success} → finished ({@code true});
     *   <li>{@code waiting for commit} / {@code running} → keep polling ({@code false}), recording the
     *       file-copy progress of the {@code running} reports into {@link DashboardState};
     *   <li>anything else, including the {@code exception} payload → throws (our backup failed).
     * </ul>
     *
     * <p>Solr publishes {@code waiting for commit} synchronously, before the trigger request returns, so a
     * previous backup's {@code success} can no longer be mistaken for ours and no start-time comparison is
     * needed to tell them apart.
     *
     * <p>Whenever a backup is reported, its snapshot directory name is captured into {@code snapshotDirectory}
     * so a failed backup's files can be located and deleted afterwards.
     */
    boolean isBackupFinished(
            JsonNode responseNode,
            AtomicReference<String> snapshotDirectory,
            String alias,
            String shardName,
            String coreName,
            String leaderUrl,
            String collection) {
        JsonNode backupNode = backupNode(responseNode);

        if (backupNode == null) {
            // No backup reported and we never saw one finish — the backup failed to complete. Fail this
            // shard rather than polling forever.
            throw new RuntimeException(
                    "Core stopped updating on backup at " + Instant.now().truncatedTo(ChronoUnit.SECONDS));
        }

        // Remember the snapshot directory Solr created for this backup so a failed one can be deleted later.
        // In-progress reports carry it too, so it is known even for an attempt that never reaches success.
        Optional.ofNullable(backupNode.get("directoryName"))
                .map(JsonNode::textValue)
                .filter(name -> !name.isBlank())
                .ifPresent(snapshotDirectory::set);

        String status = Optional.ofNullable(backupNode.get("status"))
                .map(JsonNode::textValue)
                .orElse(null);

        boolean isFinished;
        if (SUCCESS_STATUS.equalsIgnoreCase(status)) {
            isFinished = true;
        } else if (isInProgress(status)) {
            recordProgress(backupNode, alias, shardName);
            isFinished = false;
        } else {
            throw new RuntimeException("Backup status was unsuccessful" + exceptionSuffix(backupNode));
        }

        logStatus(isFinished, shardName, coreName, leaderUrl, collection);
        return isFinished;
    }

    /**
     * Records the file-copy progress of a {@code running} report. Absent on {@code waiting for commit},
     * which is published before the index commit — and with it the file list — has been resolved.
     */
    private void recordProgress(JsonNode backupNode, String alias, String shardName) {
        if (backupNode.hasNonNull("fileCount") && backupNode.hasNonNull("finishedFileCount")) {
            dashboardState.updateAttemptProgress(
                    alias,
                    shardName,
                    backupNode.get("fileCount").asInt(),
                    backupNode.get("finishedFileCount").asInt());
        }
    }

    private void logStatus(
            boolean isFinished, String shardName, String coreName, String leaderUrl, String collection) {
        log.atInfo()
                .setMessage("Shard backup status")
                .addKeyValue("is_finished", isFinished)
                .addKeyValue("shard_name", shardName)
                .addKeyValue("core_name", coreName)
                .addKeyValue("leader_url", leaderUrl)
                .addKeyValue("collection", collection)
                .log();
    }

    /**
     * Polls {@code command=details} once before triggering a backup and errors if one is already running on
     * the core. Solr does not serialize backups (a second {@code command=backup} would run in parallel against
     * a single shared status field), so we refuse to start ours while another is in progress. The error flows
     * through the caller's retry logic, so a finishing backup self-heals on a later attempt.
     *
     * <p>ponytail: fails closed. Solr holds the last reported status in memory and never resets it, so a node
     * killed mid-copy keeps reporting {@code running} until the core reloads, and this shard exhausts its
     * retries and reports {@code ERROR} instead of racing what might still be a live backup.
     */
    private Mono<Void> ensureCoreIdle(String leaderUrl, String coreName) {
        try {
            var detailsRequest = webClient
                    .get()
                    .uri(new URIBuilder(leaderUrl)
                            .setPath("solr/" + coreName + "/replication")
                            .addParameter("command", "details")
                            .build());

            return detailsRequest
                    .retrieve()
                    .bodyToMono(String.class)
                    .flatMap(response -> {
                        try {
                            if (isBackupInProgress(objectMapper.readTree(response))) {
                                return Mono.error(
                                        new RuntimeException("A backup is already in progress on this core"));
                            }
                            return Mono.empty();
                        } catch (JsonProcessingException e) {
                            return Mono.error(new RuntimeException(e));
                        }
                    })
                    .then();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private Mono<Void> checkBackupStatus(
            String leaderUrl,
            String coreName,
            String alias,
            String shardName,
            String collection,
            AtomicReference<String> snapshotDirectory) {
        try {
            var backupStatusRequest = webClient
                    .get()
                    .uri(new URIBuilder(leaderUrl)
                            .setPath("solr/" + coreName + "/replication")
                            .addParameter("command", "details")
                            .build());

            // No initial delay: Solr publishes the backup's status before the trigger request returns, so the
            // first poll always sees it.
            return backupStatusRequest
                    .retrieve()
                    .bodyToMono(String.class)
                    .repeatWhen(response -> response.delayElements(solrBackupConfiguration.getStatusEvery()))
                    .takeUntil(statusResponse -> {
                        try {
                            return isBackupFinished(
                                    objectMapper.readTree(statusResponse),
                                    snapshotDirectory,
                                    alias,
                                    shardName,
                                    coreName,
                                    leaderUrl,
                                    collection);
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .then();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private Mono<Void> sendBackupRequest(String leaderUrl, String coreName, String shardName, String alias) {
        try {
            var backupRequest = webClient
                    .get()
                    .uri(new URIBuilder(leaderUrl)
                            .setPath("solr/" + coreName + "/replication")
                            .addParameter("command", "backup")
                            .addParameter(
                                    "location",
                                    Path.of(solrBackupConfiguration.getBackupsMount(), alias, shardName)
                                            .toString()
                                            .replace("\\", "/"))
                            .build());

            return backupRequest
                    .retrieve()
                    .bodyToMono(String.class)
                    .doOnNext((response) -> log.atInfo()
                            .setMessage("Successfully started backing-up shard")
                            .addKeyValue("shard_name", shardName)
                            .addKeyValue("core_name", coreName)
                            .addKeyValue("leader_url", leaderUrl)
                            .addKeyValue("alias", alias)
                            .log())
                    .then();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates the backup directory and backs up the shard, retrying a failed attempt up to
     * {@code solr.backup.retries} times (so the shard is tried up to {@code retries + 1} times). Each
     * attempt is recorded in {@link DashboardState}; a shard that succeeds on any attempt is marked
     * {@code SUCCESS} and only a shard that fails every attempt is marked {@code ERROR}. Every failed
     * attempt's partial snapshot is deleted from disk. A failing shard never fails the whole run — one bad
     * shard never blocks the rest.
     *
     * <p>{@code leaderSupplier} is called once per attempt rather than resolved up front: a retry usually
     * follows a node or replica falling over, which is exactly when the leader moves, so retrying against
     * the leader that was current when the run started would keep hitting a replica that no longer leads.
     */
    public Mono<Void> backupShard(
            String alias, String collection, String shardName, Supplier<Replica> leaderSupplier) {
        return Mono.defer(() -> {
            try {
                Files.createDirectories(Path.of(solrBackupConfiguration.getBackupsMount(), alias, shardName));
            } catch (IOException e) {
                log.atError()
                        .setMessage("Error while creating or checking for existence of backups directory")
                        .addKeyValue("error", e.toString())
                        .addKeyValue("alias", alias)
                        .addKeyValue("collection", collection)
                        .addKeyValue("shard_name", shardName)
                        .log();
                dashboardState.markShardResult(alias, shardName, ShardStatus.ERROR, e.toString());
                return Mono.empty();
            }

            int maxRetries = solrBackupConfiguration.getRetries();
            AtomicInteger attemptNumber = new AtomicInteger(0);
            AtomicReference<String> lastError = new AtomicReference<>();

            // The leader is resolved per attempt, so these shard-level logs carry no core/leader — every
            // attempt logs the one it actually used ("Starting shard backup attempt").
            return attemptBackup(alias, collection, shardName, leaderSupplier, attemptNumber, lastError)
                    .retryWhen(Retry.fixedDelay(maxRetries, solrBackupConfiguration.getRetryDelay())
                            .doBeforeRetry(retrySignal -> log.atWarn()
                                    .setMessage("Retrying shard backup after a failed attempt")
                                    .addKeyValue("shard_name", shardName)
                                    .addKeyValue("alias", alias)
                                    .addKeyValue("collection", collection)
                                    .addKeyValue("retry", retrySignal.totalRetries() + 1)
                                    .addKeyValue("max_retries", maxRetries)
                                    .addKeyValue("previous_error", lastError.get())
                                    .log()))
                    .doOnSuccess(ignored -> {
                        log.atInfo()
                                .setMessage("Finished backing-up shard")
                                .addKeyValue("shard_name", shardName)
                                .addKeyValue("alias", alias)
                                .addKeyValue("collection", collection)
                                .addKeyValue("attempts", attemptNumber.get())
                                .log();
                        dashboardState.markShardResult(alias, shardName, ShardStatus.SUCCESS, null);
                    })
                    .onErrorResume(e -> {
                        log.atError()
                                .setMessage("Shard backup failed after exhausting all retries")
                                .addKeyValue("shard_name", shardName)
                                .addKeyValue("alias", alias)
                                .addKeyValue("collection", collection)
                                .addKeyValue("attempts", attemptNumber.get())
                                .addKeyValue("error", lastError.get())
                                .log();
                        dashboardState.markShardResult(alias, shardName, ShardStatus.ERROR, lastError.get());
                        return Mono.empty();
                    });
        });
    }

    /**
     * Runs a single backup attempt: resolves the shard's <b>current</b> leader, records the attempt as
     * {@code RUNNING}, triggers the backup, and polls until it finishes. On success the attempt is marked
     * {@code SUCCESS}; on failure the attempt is marked {@code ERROR}, its partial snapshot is deleted, and
     * the error is re-raised so the caller's retry logic can try again.
     */
    private Mono<Void> attemptBackup(
            String alias,
            String collection,
            String shardName,
            Supplier<Replica> leaderSupplier,
            AtomicInteger attemptNumber,
            AtomicReference<String> lastError) {
        return Mono.defer(() -> {
            int attempt = attemptNumber.incrementAndGet();
            AtomicReference<String> snapshotDirectory = new AtomicReference<>();
            dashboardState.startAttempt(alias, shardName, attempt);

            // Resolved inside the chain (rather than before it) so a missing leader — or a resolver that
            // throws — fails the attempt through the same error handling as a failed backup.
            return Mono.fromSupplier(leaderSupplier)
                    .switchIfEmpty(Mono.error(new RuntimeException("Shard has no elected leader")))
                    .flatMap(leader -> {
                        String coreName = leader.getCoreName();
                        String leaderUrl = leader.getCoreUrl();
                        // Keep the report pointing at the leader this attempt actually used, not the one the
                        // run started with, so a failover is visible rather than silently misreported.
                        dashboardState.updateLeader(alias, shardName, coreName, leaderUrl);

                        log.atInfo()
                                .setMessage("Starting shard backup attempt")
                                .addKeyValue("shard_name", shardName)
                                .addKeyValue("core_name", coreName)
                                .addKeyValue("leader_url", leaderUrl)
                                .addKeyValue("alias", alias)
                                .addKeyValue("collection", collection)
                                .addKeyValue("attempt", attempt)
                                .log();

                        return ensureCoreIdle(leaderUrl, coreName)
                                .then(sendBackupRequest(leaderUrl, coreName, shardName, alias))
                                .then(checkBackupStatus(
                                        leaderUrl, coreName, alias, shardName, collection, snapshotDirectory));
                    })
                    .doOnSuccess(ignored -> dashboardState.finishAttempt(alias, shardName, ShardStatus.SUCCESS, null))
                    .doOnError(e -> {
                        String message = extractErrorMessage(e);
                        log.atError()
                                .setMessage("Shard backup attempt failed")
                                .addKeyValue("error", message)
                                .addKeyValue("shard_name", shardName)
                                .addKeyValue("alias", alias)
                                .addKeyValue("collection", collection)
                                .addKeyValue("attempt", attempt)
                                .log();
                        dashboardState.finishAttempt(alias, shardName, ShardStatus.ERROR, message);
                        deleteFailedBackup(alias, shardName, snapshotDirectory.get());
                        lastError.set(message);
                    })
                    .then();
        });
    }

    private static String extractErrorMessage(Throwable e) {
        if (e instanceof WebClientResponseException responseException) {
            return responseException.getResponseBodyAsString();
        }
        return e.getMessage() != null ? e.getMessage() : e.toString();
    }

    /**
     * Deletes the partial snapshot a failed backup attempt left under {@code {backups-mount}/{alias}/{shard}},
     * so a failed replication doesn't linger on the mount. Only the specific snapshot directory Solr reported
     * for this attempt is removed — any earlier, good snapshots for the same shard are left untouched. Never
     * throws: a cleanup failure is logged, not propagated.
     */
    private void deleteFailedBackup(String alias, String shardName, String snapshotDirectory) {
        Path shardLocation = Path.of(solrBackupConfiguration.getBackupsMount(), alias, shardName);

        if (snapshotDirectory == null || snapshotDirectory.isBlank()) {
            log.atWarn()
                    .setMessage("No backup snapshot was reported for the failed attempt; nothing to delete")
                    .addKeyValue("alias", alias)
                    .addKeyValue("shard_name", shardName)
                    .addKeyValue("backup_location", shardLocation.toString())
                    .log();
            return;
        }

        // Resolve only the reported folder name under the shard location (guards against path traversal).
        Path failedBackup = shardLocation.resolve(Path.of(snapshotDirectory).getFileName());
        try {
            if (!Files.exists(failedBackup)) {
                log.atWarn()
                        .setMessage("Failed backup directory not found; nothing to delete")
                        .addKeyValue("alias", alias)
                        .addKeyValue("shard_name", shardName)
                        .addKeyValue("backup_path", failedBackup.toString())
                        .log();
                return;
            }

            deleteRecursively(failedBackup);
            log.atInfo()
                    .setMessage("Deleted failed backup")
                    .addKeyValue("alias", alias)
                    .addKeyValue("shard_name", shardName)
                    .addKeyValue("deleted_path", failedBackup.toString())
                    .log();
        } catch (IOException e) {
            log.atError()
                    .setMessage("Failed to delete failed backup; manual cleanup may be required")
                    .addKeyValue("error", e.toString())
                    .addKeyValue("alias", alias)
                    .addKeyValue("shard_name", shardName)
                    .addKeyValue("backup_path", failedBackup.toString())
                    .log();
        }
    }

    private static void deleteRecursively(Path root) throws IOException {
        try (Stream<Path> paths = Files.walk(root)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.delete(path);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }
}
