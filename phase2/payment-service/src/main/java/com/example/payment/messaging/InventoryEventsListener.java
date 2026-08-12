package com.example.payment.messaging;

import com.example.payment.event.StockReservationFailedEvent;
import com.example.payment.event.StockReservedEvent;
import com.example.payment.service.PaymentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * "inventory-events" carries both StockReservedEvent and
 * StockReservationFailedEvent, so - same pattern as order-service and
 * inventory-service's own dual-event-topic listeners - this uses
 * class-level @KafkaListener + @KafkaHandler dispatch, ONE listener
 * container, group "payment-service".
 *
 * payment-service only acts on StockReservedEvent (that's the signal
 * it's safe to charge). StockReservationFailedEvent needs no action -
 * if stock reservation failed, the order never reaches payment at all,
 * so there's nothing here to do or undo.
 */
@Component
@KafkaListener(topics = "inventory-events", groupId = "payment-service")
public class InventoryEventsListener {

    private static final Logger log = LoggerFactory.getLogger(InventoryEventsListener.class);

    private final PaymentService paymentService;

    public InventoryEventsListener(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @KafkaHandler
    public void onStockReserved(StockReservedEvent event) {
        log.info("Received StockReservedEvent for orderId={}, attempting payment", event.getOrderId());
        paymentService.processPayment(event.getOrderId(), event.getProductId(), event.getQuantity());
    }

    @KafkaHandler
    public void onStockReservationFailed(StockReservationFailedEvent event) {
        // No-op: order never reached payment, nothing to do here.
        log.debug("Received StockReservationFailedEvent for orderId={}, no action needed", event.getOrderId());
    }

    @KafkaHandler(isDefault = true)
    public void onUnhandled(Object event) {
        log.warn("Received unrecognized event on inventory-events: {}", event);
    }
}