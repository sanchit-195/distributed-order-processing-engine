package com.example.order.messaging;

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
        // Must match the logical names used in every consumer's
        // spring.json.type.mapping, NOT the fully-qualified class name -
        // that's what lets order-service, inventory-service, and
        // payment-service each keep their own copy of the event class
        // in their own package and still understand each other's messages.
        props.put(JsonSerializer.TYPE_MAPPINGS,
                "orderCreated:com.example.order.event.OrderCreatedEvent," +
                "stockReserved:com.example.order.event.StockReservedEvent," +
                "stockReservationFailed:com.example.order.event.StockReservationFailedEvent," +
                "paymentCompleted:com.example.order.event.PaymentCompletedEvent," +
                "paymentFailed:com.example.order.event.PaymentFailedEvent");
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}