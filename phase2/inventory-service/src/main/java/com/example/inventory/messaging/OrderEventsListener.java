package com.example.inventory.messaging;

import com.example.inventory.event.OrderCreatedEvent;
import com.example.inventory.service.InventoryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Single listener on "order-events", group "inventory-service". Only
 * one event type flows through this topic today (OrderCreatedEvent), so
 * no @KafkaHandler dispatch is needed here - a plain typed @KafkaListener
 * is enough. If order-service ever publishes a second event type to this
 * topic later, this would need the same class-level @KafkaListener +
 * @KafkaHandler pattern used in order-service's InventoryEventsListener.
 */
@Component
public class OrderEventsListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventsListener.class);

    private final InventoryService inventoryService;

    public OrderEventsListener(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    @KafkaListener(topics = "order-events", groupId = "inventory-service")
    public void onOrderCreated(OrderCreatedEvent event) {
        log.info("Received OrderCreatedEvent for orderId={}, productId={}, quantity={}",
                event.getOrderId(), event.getProductId(), event.getQuantity());
        inventoryService.reserveStock(event.getOrderId(), event.getProductId(), event.getQuantity());
    }
}
