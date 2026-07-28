package com.example.order.messaging;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares the topic order-service publishes to. Spring Kafka's
 * KafkaAdmin bean (auto-configured) will create this on startup if it
 * doesn't already exist - convenient for local dev, though a real
 * deployment would usually pre-provision topics via Terraform/scripts
 * instead of leaving creation to app startup.
 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic orderEventsTopic() {
        return TopicBuilder.name("order-events")
                .partitions(3)
                .replicas(1)
                .build();
    }
}