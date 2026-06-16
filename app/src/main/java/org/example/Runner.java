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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.configuration.SolrBackupConfiguration;
import org.apache.http.client.utils.URIBuilder;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
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

    public Mono<Void> sendBackupRequest(Replica leader, Slice targetSlice) {
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
                                                    targetSlice.getCollection(),
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
                            .addKeyValue("collection", targetSlice.getCollection())
                            .log())
                    .then();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private Mono<Void> startBackupForShard(Slice slice, Replica leader, String leaderUrl) {
        return Mono.defer(() -> {
                    log.atInfo()
                            .setMessage("Starting to backup shard")
                            .addKeyValue("shard_name", slice.getName())
                            .addKeyValue("core_name", leader.getCoreName())
                            .addKeyValue("leader_url", leaderUrl)
                            .addKeyValue("collection", slice.getCollection())
                            .log();

                    return sendBackupRequest(leader, slice)
                            .then(checkBackupStatus(leader, slice))
                            .doOnSuccess(signal -> log.atInfo()
                                    .setMessage("Finished backing-up shard")
                                    .addKeyValue("shard_name", slice.getName())
                                    .addKeyValue("core_name", leader.getCoreName())
                                    .addKeyValue("leader_url", leaderUrl)
                                    .addKeyValue("collection", slice.getCollection())
                                    .log());
                })
                .doOnError(e -> {
                    var errorLogBase = log.atError()
                            .setMessage("Encountered error while running backup for shard")
                            .addKeyValue("error", e.toString())
                            .addKeyValue("shard_name", slice.getName())
                            .addKeyValue("core_name", leader.getCoreName())
                            .addKeyValue("leader_url", leaderUrl)
                            .addKeyValue("collection", slice.getCollection());

                    if (e instanceof WebClientResponseException responseException) {
                        errorLogBase = errorLogBase.addKeyValue(
                                "response_message", responseException.getResponseBodyAsString());
                    }

                    errorLogBase.log();
                })
                .onErrorComplete();
    }

    @Override
    public void run(ApplicationArguments args) throws IOException {
        log.info("Initiating backups");

        try (CloudSolrClient client = new CloudSolrClient.Builder(
                        Collections.singletonList(solrBackupConfiguration.getZookeeper()), Optional.empty())
                .build()) {
            Flux.fromIterable(client.getClusterState().getCollectionsMap().values())
                    .filter(collection -> !collection.getName().startsWith(".sys"))
                    .filter(collection -> Optional.ofNullable(solrBackupConfiguration.getWhitelistCollections())
                            .map(whitelist -> whitelist.contains(collection.getName()))
                            .orElse(false))
                    .delayElements(Duration.ofSeconds(15))
                    .flatMapIterable(Function.identity())
                    .<Slice>handle((slice, sink) -> {
                        try {
                            Files.createDirectories(Path.of(
                                    solrBackupConfiguration.getBackupsMount(), slice.getCollection(), slice.getName()));
                            sink.next(slice);
                        } catch (IOException e) {
                            log.atError()
                                    .setMessage("Error while creating or checking for existence of backups directory")
                                    .addKeyValue("error", e)
                                    .addKeyValue("collection", slice.getCollection())
                                    .addKeyValue("shard_name", slice.getName())
                                    .log();
                        }
                    })
                    .flatMap(
                            slice -> {
                                Replica leader = slice.getLeader();
                                String leaderUrl = leader.getCoreUrl();

                                return startBackupForShard(slice, leader, leaderUrl);
                            },
                            solrBackupConfiguration.getParallelBackups())
                    .subscribeOn(Schedulers.boundedElastic())
                    .doOnComplete(() -> log.info("Finished backups"))
                    .blockLast();
            System.exit(0);
        }
    }
}
