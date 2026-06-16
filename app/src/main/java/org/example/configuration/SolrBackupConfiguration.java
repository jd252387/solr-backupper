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
    private List<String> whitelistCollections;
    private String backupsMount;
    private int parallelBackups;
    private Duration statusEvery;
    private Duration maxEndTimeDelta;
}
