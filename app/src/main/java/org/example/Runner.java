/* (C)Team Eclipse 2024 */
package org.example;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.configuration.SolrBackupConfiguration;
import org.example.dashboard.DashboardState;
import org.example.report.BackupReportWriter;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Service
@RequiredArgsConstructor
@Slf4j
public class Runner implements ApplicationRunner {
    private final SolrBackupConfiguration solrBackupConfiguration;
    private final ShardBackupExecutor shardBackupExecutor;
    private final DashboardState dashboardState;
    private final BackupReportWriter backupReportWriter;

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

        // Rewrite the live report file on a fixed interval for the whole run so the standalone
        // dashboard (reading the shared report directory) can watch the run progress in near real time.
        ScheduledExecutorService reportScheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "live-report-writer");
            thread.setDaemon(true);
            return thread;
        });
        long reportIntervalMillis = solrBackupConfiguration.getReportUpdateInterval().toMillis();
        reportScheduler.scheduleAtFixedRate(
                backupReportWriter::writeReport, reportIntervalMillis, reportIntervalMillis, TimeUnit.MILLISECONDS);

        try (CloudSolrClient client = new CloudSolrClient.Builder(
                        Collections.singletonList(solrBackupConfiguration.getZookeeper()), Optional.empty())
                .build()) {
            ClusterState clusterState = client.getClusterState();
            Map<String, List<String>> aliasToCollections =
                    ZkStateReader.from(client).getAliases().getCollectionAliasListMap();

            // Resolve every whitelisted alias to its target collection up front.
            List<AliasTarget> targets = Optional.ofNullable(solrBackupConfiguration.getWhitelistAliases())
                    .orElse(List.of())
                    .stream()
                    .flatMap(alias -> resolveAlias(alias, aliasToCollections, clusterState).stream())
                    .toList();

            // Register every shard as PENDING and publish an initial report immediately, so the dashboard
            // shows the whole run (all shards pending) from the very start — before any backup begins —
            // instead of shards only appearing once they are already running or finished.
            targets.forEach(target -> target.collection().getSlices().forEach(slice -> {
                Replica leader = slice.getLeader();
                dashboardState.registerPending(
                        target.alias(),
                        slice.getCollection(),
                        slice.getName(),
                        leader.getCoreName(),
                        leader.getCoreUrl());
            }));
            backupReportWriter.writeReport();

            Flux.fromIterable(targets)
                    // Stagger aliases by between-aliases-delay to spread load; the first starts immediately.
                    .index()
                    .concatMap(indexed -> Mono.just(indexed.getT2())
                            .delaySubscription(indexed.getT1() == 0L
                                    ? Duration.ZERO
                                    : solrBackupConfiguration.getBetweenAliasesDelay()))
                    .flatMapIterable(target -> target.collection().getSlices().stream()
                            .map(slice -> new AliasedSlice(target.alias(), slice))
                            .toList())
                    .flatMap(
                            aliasedSlice -> {
                                Slice slice = aliasedSlice.slice();
                                Replica leader = slice.getLeader();

                                return shardBackupExecutor.backupShard(
                                        aliasedSlice.alias(),
                                        slice.getCollection(),
                                        slice.getName(),
                                        leader.getCoreName(),
                                        leader.getCoreUrl());
                            },
                            solrBackupConfiguration.getParallelBackups())
                    .subscribeOn(Schedulers.boundedElastic())
                    .doOnComplete(() -> log.info("Finished backups"))
                    .then()
                    .block();
        } finally {
            // Stop the periodic writer and let any in-flight write finish before the final flush below,
            // so the terminal-status report is guaranteed to be the last one written for this run.
            reportScheduler.shutdown();
            try {
                reportScheduler.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        dashboardState.markRunFinished();
        backupReportWriter.writeReport();
        System.exit(0);
    }

    private record AliasTarget(String alias, DocCollection collection) {}

    private record AliasedSlice(String alias, Slice slice) {}
}
