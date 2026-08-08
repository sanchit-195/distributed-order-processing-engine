package com.example.inventory.messaging;

import com.example.inventory.event.StockReservationFailedEvent;
import com.example.inventory.event.StockReservedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class InventoryEventProducer {

    private static final Logger log = LoggerFactory.getLogger(InventoryEventProducer.class);
    private static final String TOPIC = "inventory-events";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public InventoryEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishStockReserved(StockReservedEvent event) {
        String key = String.valueOf(event.getOrderId());
        log.info("Publishing StockReservedEvent for orderId={}", event.getOrderId());
        kafkaTemplate.send(TOPIC, key, event);
    }

    public void publishStockReservationFailed(StockReservationFailedEvent event) {
        String key = String.valueOf(event.getOrderId());
        log.info("Publishing StockReservationFailedEvent for orderId={}: {}",
                event.getOrderId(), event.getReason());
        kafkaTemplate.send(TOPIC, key, event);
    }
}
