/* (C)Team Eclipse 2024 */
package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.configuration.SolrBackupConfiguration;
import org.example.report.BackupOutcome;
import org.example.report.BackupReportWriter;
import org.example.report.ShardBackupResult;
import org.apache.http.client.utils.URIBuilder;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Service
@RequiredArgsConstructor
@Slf4j
public class Runner implements ApplicationRunner {
    private final SolrBackupConfiguration solrBackupConfiguration;
    private final WebClient webClient;
    private final ObjectMapper objectMapper;
    private final BackupReportWriter backupReportWriter;

    public boolean isEndTimeBelowAllowedDelta(LocalDateTime lastFinishedAt) {
        return Duration.between(lastFinishedAt, ZonedDateTime.now(ZoneOffset.UTC))
                        .compareTo(solrBackupConfiguration.getMaxEndTimeDelta())
                <= 0;
    }

    public boolean isBackupFinished(JsonNode responseNode, Replica leader, Slice targetSlice) {
        boolean shouldContinue = Optional.ofNullable(responseNode.get("details"))
                .map(detailsNode -> detailsNode.get("backup"))
                .map(backupNode -> Optional.ofNullable(backupNode.get("endTime"))
                        .map(JsonNode::textValue)
                        .map(dateStr -> LocalDateTime.parse(dateStr, DateTimeFormatter.ISO_DATE_TIME))
                        .map(this::isEndTimeBelowAllowedDelta)
                        .flatMap(isFinished -> isFinished
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
                                        throw new RuntimeException(
                                                "Backup encountered an exception - " + exceptionValue);
                                    }));
                            return false;
                        }))
                .orElse(false);

        log.atInfo()
                .setMessage("Shard backup status")
                .addKeyValue("is_finished", shouldContinue)
                .addKeyValue("shard_name", targetSlice.getName())
                .addKeyValue("core_name", leader.getCoreName())
                .addKeyValue("leader_url", leader.getCoreUrl())
                .addKeyValue("collection", targetSlice.getCollection())
                .log();

        return shouldContinue;
    }

    public Mono<Void> checkBackupStatus(Replica leader, Slice targetSlice) {
        try {
            var backupStatusRequest = webClient
                    .get()
                    .uri(new URIBuilder(leader.getCoreUrl())
                            .setPath("solr/" + leader.getCoreName() + "/replication")
                            .addParameter("command", "details")
                            .build());

            return Mono.defer(() -> backupStatusRequest
                    .retrieve()
                    .bodyToMono(String.class)
                    .repeatWhen(response -> response.delayElements(solrBackupConfiguration.getStatusEvery()))
                    .takeUntil(statusResponse -> {
                        try {
                            return isBackupFinished(objectMapper.readTree(statusResponse), leader, targetSlice);
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .then());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public Mono<Void> sendBackupRequest(Replica leader, Slice targetSlice, String alias) {
        try {
            var backupRequest = webClient
                    .get()
                    .uri(new URIBuilder(leader.getCoreUrl())
                            .setPath("solr/" + leader.getCoreName() + "/replication")
                            .addParameter("command", "backup")
                            .addParameter(
                                    "location",
                                    Path.of(
                                                    solrBackupConfiguration.getBackupsMount(),
                                                    alias,
                                                    targetSlice.getName())
                                            .toString()
                                            .replace("\\", "/"))
                            .build());

            return backupRequest
                    .retrieve()
                    .bodyToMono(String.class)
                    .doOnNext((response) -> log.atInfo()
                            .setMessage("Successfully started backing-up shard")
                            .addKeyValue("shard_name", targetSlice.getName())
                            .addKeyValue("core_name", leader.getCoreName())
                            .addKeyValue("leader_url", leader.getCoreUrl())
                            .addKeyValue("alias", alias)
                            .addKeyValue("collection", targetSlice.getCollection())
                            .log())
                    .then();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private ShardBackupResult shardResult(
            Slice slice, Replica leader, String leaderUrl, String alias, BackupOutcome outcome, Instant startedAt) {
        return new ShardBackupResult(
                alias,
                slice.getCollection(),
                slice.getName(),
                leader.getCoreName(),
                leaderUrl,
                outcome,
                startedAt,
                Instant.now());
    }

    private Mono<ShardBackupResult> startBackupForShard(
            Slice slice, Replica leader, String leaderUrl, String alias) {
        return Mono.defer(() -> {
                    Instant startedAt = Instant.now();

                    log.atInfo()
                            .setMessage("Starting to backup shard")
                            .addKeyValue("shard_name", slice.getName())
                            .addKeyValue("core_name", leader.getCoreName())
                            .addKeyValue("leader_url", leaderUrl)
                            .addKeyValue("alias", alias)
                            .addKeyValue("collection", slice.getCollection())
                            .log();

                    return sendBackupRequest(leader, slice, alias)
                            .then(checkBackupStatus(leader, slice))
                            .timeout(solrBackupConfiguration.getBackupTimeout())
                            .then(Mono.fromSupplier(() -> {
                                log.atInfo()
                                        .setMessage("Finished backing-up shard")
                                        .addKeyValue("shard_name", slice.getName())
                                        .addKeyValue("core_name", leader.getCoreName())
                                        .addKeyValue("leader_url", leaderUrl)
                                        .addKeyValue("alias", alias)
                                        .addKeyValue("collection", slice.getCollection())
                                        .log();
                                return shardResult(
                                        slice, leader, leaderUrl, alias, BackupOutcome.SUCCESS, startedAt);
                            }))
                            .onErrorResume(TimeoutException.class, e -> {
                                log.atWarn()
                                        .setMessage(
                                                "Timed out waiting for shard backup to complete; moving on to next shard")
                                        .addKeyValue("backup_timeout", solrBackupConfiguration.getBackupTimeout())
                                        .addKeyValue("shard_name", slice.getName())
                                        .addKeyValue("core_name", leader.getCoreName())
                                        .addKeyValue("leader_url", leaderUrl)
                                        .addKeyValue("alias", alias)
                                        .addKeyValue("collection", slice.getCollection())
                                        .log();
                                return Mono.fromSupplier(() -> shardResult(
                                        slice, leader, leaderUrl, alias, BackupOutcome.TIMED_OUT, startedAt));
                            })
                            .onErrorResume(e -> {
                                var errorLogBase = log.atError()
                                        .setMessage("Encountered error while running backup for shard")
                                        .addKeyValue("error", e.toString())
                                        .addKeyValue("shard_name", slice.getName())
                                        .addKeyValue("core_name", leader.getCoreName())
                                        .addKeyValue("leader_url", leaderUrl)
                                        .addKeyValue("alias", alias)
                                        .addKeyValue("collection", slice.getCollection());

                                if (e instanceof WebClientResponseException responseException) {
                                    errorLogBase = errorLogBase.addKeyValue(
                                            "response_message", responseException.getResponseBodyAsString());
                                }

                                errorLogBase.log();
                                return Mono.fromSupplier(() -> shardResult(
                                        slice, leader, leaderUrl, alias, BackupOutcome.ERROR, startedAt));
                            });
                });
    }

    private List<AliasTarget> resolveAlias(
            String alias, Map<String, List<String>> aliasToCollections, ClusterState clusterState) {
        List<String> collections = aliasToCollections.get(alias);

        if (collections == null || collections.isEmpty()) {
            log.atError()
                    .setMessage("Alias not found in Solr or points to no collections")
                    .addKeyValue("alias", alias)
                    .log();
            return List.of();
        }
        if (collections.size() > 1) {
            log.atError()
                    .setMessage("Alias points to multiple collections; skipping backup")
                    .addKeyValue("alias", alias)
                    .addKeyValue("collections", collections)
                    .log();
            return List.of();
        }

        String collectionName = collections.get(0);
        DocCollection collection = clusterState.getCollectionOrNull(collectionName);
        if (collection == null) {
            log.atError()
                    .setMessage("Collection referenced by alias not found in cluster state")
                    .addKeyValue("alias", alias)
                    .addKeyValue("collection", collectionName)
                    .log();
            return List.of();
        }
        return List.of(new AliasTarget(alias, collection));
    }

    @Override
    public void run(ApplicationArguments args) throws IOException {
        log.info("Initiating backups");

        try (CloudSolrClient client = new CloudSolrClient.Builder(
                        Collections.singletonList(solrBackupConfiguration.getZookeeper()), Optional.empty())
                .build()) {
            ClusterState clusterState = client.getClusterState();
            Map<String, List<String>> aliasToCollections =
                    ZkStateReader.from(client).getAliases().getCollectionAliasListMap();

            Flux.fromIterable(Optional.ofNullable(solrBackupConfiguration.getWhitelistAliases())
                            .orElse(List.of()))
                    .flatMapIterable(alias -> resolveAlias(alias, aliasToCollections, clusterState))
                    .delayElements(Duration.ofSeconds(15))
                    .flatMapIterable(target -> target.collection().getSlices().stream()
                            .map(slice -> new AliasedSlice(target.alias(), slice))
                            .toList())
                    .<AliasedSlice>handle((aliasedSlice, sink) -> {
                        Slice slice = aliasedSlice.slice();
                        try {
                            Files.createDirectories(Path.of(
                                    solrBackupConfiguration.getBackupsMount(),
                                    aliasedSlice.alias(),
                                    slice.getName()));
                            sink.next(aliasedSlice);
                        } catch (IOException e) {
                            log.atError()
                                    .setMessage("Error while creating or checking for existence of backups directory")
                                    .addKeyValue("error", e)
                                    .addKeyValue("alias", aliasedSlice.alias())
                                    .addKeyValue("collection", slice.getCollection())
                                    .addKeyValue("shard_name", slice.getName())
                                    .log();
                        }
                    })
                    .flatMap(
                            aliasedSlice -> {
                                Slice slice = aliasedSlice.slice();
                                Replica leader = slice.getLeader();
                                String leaderUrl = leader.getCoreUrl();

                                return startBackupForShard(slice, leader, leaderUrl, aliasedSlice.alias());
                            },
                            solrBackupConfiguration.getParallelBackups())
                    .subscribeOn(Schedulers.boundedElastic())
                    .doOnComplete(() -> log.info("Finished backups"))
                    .collectList()
                    .doOnNext(backupReportWriter::writeReport)
                    .block();
            System.exit(0);
        }
    }

    private record AliasTarget(String alias, DocCollection collection) {}

    private record AliasedSlice(String alias, Slice slice) {}
}
