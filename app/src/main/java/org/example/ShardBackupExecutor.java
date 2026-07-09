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
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.utils.URIBuilder;
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
 * drives the full-run pipeline) so it works from plain identifying strings rather than live
 * {@code Slice}/{@code Replica} objects tied to a single run's ZooKeeper session.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ShardBackupExecutor {
    private final SolrBackupConfiguration solrBackupConfiguration;
    private final WebClient webClient;
    private final ObjectMapper objectMapper;
    private final DashboardState dashboardState;

    private boolean isEndTimeBelowAllowedDelta(LocalDateTime lastFinishedAt) {
        return Duration.between(lastFinishedAt, ZonedDateTime.now(ZoneOffset.UTC))
                        .compareTo(solrBackupConfiguration.getMaxEndTimeDelta())
                <= 0;
    }

    /**
     * Evaluates one {@code command=details} poll:
     *
     * <ul>
     *   <li>backup present but not yet complete → still running, returns {@code false} (keep polling);
     *   <li>backup present and finished → returns {@code true} on success, throws on failure/exception;
     *   <li>no backup reported at all → throws, because we never saw one finish, so it never completed
     *       (it was never registered, or the core reloaded and dropped it).
     * </ul>
     *
     * <p>Whenever a backup is reported, its snapshot directory name is captured into {@code snapshotDirectory}
     * so a failed backup's files can be located and deleted afterwards.
     */
    private boolean isBackupFinished(
            JsonNode responseNode,
            AtomicReference<String> snapshotDirectory,
            String shardName,
            String coreName,
            String leaderUrl,
            String collection) {
        JsonNode backupNode = Optional.ofNullable(responseNode.get("details"))
                .map(detailsNode -> detailsNode.get("backup"))
                .filter(node -> !node.isNull())
                .orElse(null);

        if (backupNode == null) {
            // No backup reported and we never saw one finish — the backup failed to complete. Fail this
            // shard rather than polling forever.
            throw new RuntimeException(
                    "Core stopped updating on backup at " + Instant.now().truncatedTo(ChronoUnit.SECONDS));
        }

        // Remember the snapshot directory Solr created for this backup so a failed one can be deleted later.
        Optional.ofNullable(backupNode.get("directoryName"))
                .map(JsonNode::textValue)
                .filter(name -> !name.isBlank())
                .ifPresent(snapshotDirectory::set);

        boolean isFinished = Optional.ofNullable(backupNode.get("endTime"))
                .map(JsonNode::textValue)
                .map(dateStr -> LocalDateTime.parse(dateStr, DateTimeFormatter.ISO_DATE_TIME))
                .map(this::isEndTimeBelowAllowedDelta)
                .flatMap(isFresh -> isFresh
                        ? Optional.ofNullable(backupNode.get("status"))
                                .map(JsonNode::textValue)
                                .map(value -> value.equals("success"))
                                .map(isSuccess -> {
                                    if (!isSuccess) {
                                        throw new RuntimeException("Backup status was unsuccessful");
                                    }

                                    return true;
                                })
                        : Optional.of(false))
                .orElseGet(() -> {
                    Optional.ofNullable(backupNode.get(1))
                            .map(JsonNode::textValue)
                            .ifPresent((exceptionValue -> {
                                throw new RuntimeException("Backup encountered an exception - " + exceptionValue);
                            }));
                    return false;
                });

        logStatus(isFinished, shardName, coreName, leaderUrl, collection);
        return isFinished;
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

    private Mono<Void> checkBackupStatus(
            String leaderUrl,
            String coreName,
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

            // Wait before the first poll so the freshly triggered backup has time to register on the core;
            // otherwise an immediate "no backup" reading would wrongly fail the shard.
            return Mono.delay(solrBackupConfiguration.getInitialStatusDelay())
                    .then(Mono.defer(() -> backupStatusRequest
                            .retrieve()
                            .bodyToMono(String.class)
                            .repeatWhen(response -> response.delayElements(solrBackupConfiguration.getStatusEvery()))
                            .takeUntil(statusResponse -> {
                                try {
                                    return isBackupFinished(
                                            objectMapper.readTree(statusResponse),
                                            snapshotDirectory,
                                            shardName,
                                            coreName,
                                            leaderUrl,
                                            collection);
                                } catch (JsonProcessingException e) {
                                    throw new RuntimeException(e);
                                }
                            })
                            .then()));
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
     */
    public Mono<Void> backupShard(
            String alias, String collection, String shardName, String coreName, String leaderUrl) {
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

            return attemptBackup(alias, collection, shardName, coreName, leaderUrl, attemptNumber, lastError)
                    .retryWhen(Retry.fixedDelay(maxRetries, solrBackupConfiguration.getRetryDelay())
                            .doBeforeRetry(retrySignal -> log.atWarn()
                                    .setMessage("Retrying shard backup after a failed attempt")
                                    .addKeyValue("shard_name", shardName)
                                    .addKeyValue("core_name", coreName)
                                    .addKeyValue("leader_url", leaderUrl)
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
                                .addKeyValue("core_name", coreName)
                                .addKeyValue("leader_url", leaderUrl)
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
                                .addKeyValue("core_name", coreName)
                                .addKeyValue("leader_url", leaderUrl)
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
     * Runs a single backup attempt: records the attempt as {@code RUNNING}, triggers the backup, and polls
     * until it finishes. On success the attempt is marked {@code SUCCESS}; on failure the attempt is marked
     * {@code ERROR}, its partial snapshot is deleted, and the error is re-raised so the caller's retry
     * logic can try again.
     */
    private Mono<Void> attemptBackup(
            String alias,
            String collection,
            String shardName,
            String coreName,
            String leaderUrl,
            AtomicInteger attemptNumber,
            AtomicReference<String> lastError) {
        return Mono.defer(() -> {
            int attempt = attemptNumber.incrementAndGet();
            AtomicReference<String> snapshotDirectory = new AtomicReference<>();
            dashboardState.startAttempt(alias, shardName, attempt);

            log.atInfo()
                    .setMessage("Starting shard backup attempt")
                    .addKeyValue("shard_name", shardName)
                    .addKeyValue("core_name", coreName)
                    .addKeyValue("leader_url", leaderUrl)
                    .addKeyValue("alias", alias)
                    .addKeyValue("collection", collection)
                    .addKeyValue("attempt", attempt)
                    .log();

            return sendBackupRequest(leaderUrl, coreName, shardName, alias)
                    .then(checkBackupStatus(leaderUrl, coreName, shardName, collection, snapshotDirectory))
                    .doOnSuccess(ignored -> dashboardState.finishAttempt(alias, shardName, ShardStatus.SUCCESS, null))
                    .doOnError(e -> {
                        String message = extractErrorMessage(e);
                        log.atError()
                                .setMessage("Shard backup attempt failed")
                                .addKeyValue("error", message)
                                .addKeyValue("shard_name", shardName)
                                .addKeyValue("core_name", coreName)
                                .addKeyValue("leader_url", leaderUrl)
                                .addKeyValue("alias", alias)
                                .addKeyValue("collection", collection)
                                .addKeyValue("attempt", attempt)
                                .log();
                        dashboardState.finishAttempt(alias, shardName, ShardStatus.ERROR, message);
                        deleteFailedBackup(alias, shardName, snapshotDirectory.get());
                        lastError.set(message);
                    });
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
