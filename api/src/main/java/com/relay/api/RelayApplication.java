package com.relay.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = {"com.relay.api", "com.relay.core"})
@EnableScheduling
@EntityScan(basePackages = "com.relay.core.model")
@EnableJpaRepositories(basePackages = "com.relay.core.repository")
public class RelayApplication {
    public static void main(String[] args) {
        SpringApplication.run(RelayApplication.class, args);
    }
}
