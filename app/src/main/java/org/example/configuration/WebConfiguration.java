/* (C)Team Eclipse 2024 */
package org.example.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

@Configuration
@RequiredArgsConstructor
public class WebConfiguration {
    @Value("${solr.auth.username}")
    private String basicAuthUsername;

    @Value("${solr.auth.password}")
    private String basicAuthPassword;

    @Bean
    public WebClient createWebClient() {
        var httpClient = HttpClient.create();

        return WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .defaultHeaders(headers -> headers.setBasicAuth(basicAuthUsername, basicAuthPassword))
                .build();
    }

    @Bean
    public ObjectMapper createObjectMapper() {
        // findAndRegisterModules() picks up jackson-datatype-jsr310 (already on the classpath via
        // spring-boot-starter-json) so Instant fields serialize as ISO-8601 instead of falling back to
        // reflection over Instant's own getEpochSecond()/getNano() accessors.
        return new ObjectMapper().findAndRegisterModules().disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }
}
