package com.example.inventory.messaging;

import com.example.inventory.event.PaymentCompletedEvent;
import com.example.inventory.event.PaymentFailedEvent;
import com.example.inventory.service.InventoryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * "payment-events" carries both PaymentCompletedEvent and
 * PaymentFailedEvent, so - same as order-service's PaymentEventsListener -
 * this uses class-level @KafkaListener + @KafkaHandler dispatch, one
 * listener container, group "inventory-service".
 *
 * inventory-service only cares about PaymentFailedEvent (triggers the
 * compensating release). PaymentCompletedEvent needs no action here -
 * inventory-service already did its job when it reserved the stock;
 * there's nothing left to do on the happy path.
 */
@Component
@KafkaListener(topics = "payment-events", groupId = "inventory-service")
public class PaymentEventsListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventsListener.class);

    private final InventoryService inventoryService;

    public PaymentEventsListener(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    @KafkaHandler
    public void onPaymentFailed(PaymentFailedEvent event) {
        log.info("Received PaymentFailedEvent for orderId={}, triggering stock release", event.getOrderId());
        inventoryService.releaseStock(event.getOrderId());
    }

    @KafkaHandler
    public void onPaymentCompleted(PaymentCompletedEvent event) {
        // No-op: nothing for inventory-service to do on payment success.
        log.debug("Received PaymentCompletedEvent for orderId={}, no action needed", event.getOrderId());
    }

    @KafkaHandler(isDefault = true)
    public void onUnhandled(Object event) {
        log.warn("Received unrecognized event on payment-events: {}", event);
    }
}
