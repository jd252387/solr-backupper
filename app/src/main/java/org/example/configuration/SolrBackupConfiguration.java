/* (C)Team Eclipse 2024 */
package org.example.configuration;

import java.time.Duration;
import java.util.List;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties("solr.backup")
public class SolrBackupConfiguration {
    private String zookeeper;
    private List<String> whitelistAliases;
    private String backupsMount;
    private int parallelBackups;
    private Duration statusEvery;
    private String reportOutputDirectory;

    /**
     * How often the live backup report file is rewritten during a run. Defaulted (unlike the required
     * properties above) so existing deployments keep working without adding this key. The report is
     * read by the standalone dashboard container off a shared volume, so a shorter interval means a
     * livelier UI at the cost of slightly more I/O.
     */
    private Duration reportUpdateInterval = Duration.ofSeconds(5);

    /**
     * Delay before the first {@code command=details} status poll of a freshly triggered backup, giving the
     * core time to register it so an initial poll never reads "no backup" and wrongly fails the shard.
     * Defaulted (like {@link #reportUpdateInterval}) so existing config still binds.
     */
    private Duration initialStatusDelay = Duration.ofSeconds(3);

    /**
     * Delay before starting each alias's backups after the first, to stagger load across the cluster (the
     * first alias starts immediately). Defaulted (like {@link #reportUpdateInterval}) so existing config
     * still binds.
     */
    private Duration betweenAliasesDelay = Duration.ofSeconds(15);

    /**
     * How many times to retry a shard's backup after a failed attempt, so a shard is tried up to
     * {@code retries + 1} times in total. A shard that succeeds on any attempt is {@code SUCCESS}; only a
     * shard that fails every attempt is {@code ERROR}. Defaulted so existing config still binds.
     */
    private int retries = 2;

    /**
     * Delay before retrying a shard's backup after a failed attempt. Defaulted (like {@link #retries}) so
     * existing config still binds.
     */
    private Duration retryDelay = Duration.ofSeconds(10);
}
