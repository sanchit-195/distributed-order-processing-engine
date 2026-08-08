package com.example.inventory.messaging;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        // Logical type names - MUST match the keys every consumer
        // (order-service, payment-service) has in their own
        // spring.json.type.mapping, even though the class on THIS side
        // lives in com.example.inventory.event.
        props.put(JsonSerializer.TYPE_MAPPINGS,
                "orderCreated:com.example.inventory.event.OrderCreatedEvent," +
                "stockReserved:com.example.inventory.event.StockReservedEvent," +
                "stockReservationFailed:com.example.inventory.event.StockReservationFailedEvent," +
                "paymentCompleted:com.example.inventory.event.PaymentCompletedEvent," +
                "paymentFailed:com.example.inventory.event.PaymentFailedEvent");
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
