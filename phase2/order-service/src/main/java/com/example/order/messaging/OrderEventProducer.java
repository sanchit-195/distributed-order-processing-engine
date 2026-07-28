package com.example.order.messaging;

import com.example.order.event.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class OrderEventProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventProducer.class);
    private static final String TOPIC = "order-events";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishOrderCreated(OrderCreatedEvent event) {
        // Keying by orderId (as a string) ensures every event for the
        // same order lands on the same partition, so consumers that
        // care about ordering per-order see them in order.
        String key = String.valueOf(event.getOrderId());
        log.info("Publishing OrderCreatedEvent for orderId={}", event.getOrderId());
        kafkaTemplate.send(TOPIC, key, event);
    }
}