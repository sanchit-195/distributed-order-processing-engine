package com.example.order.messaging;

import com.example.order.entity.Order;
import com.example.order.event.InventoryRestoreEvent;
import com.example.order.event.StockReservationFailedEvent;
import com.example.order.event.StockReservedEvent;
import com.example.order.repository.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * A single listener container subscribed to "inventory-events" as group
 * "order-service". StockReservedEvent, StockReservationFailedEvent, and
 * InventoryRestoreEvent flow through this one topic, dispatched by runtime type.
 */
@Component
@KafkaListener(topics = "inventory-events", groupId = "order-service")
public class InventoryEventsListener {

    private static final Logger log = LoggerFactory.getLogger(InventoryEventsListener.class);

    private final OrderRepository orderRepository;

    public InventoryEventsListener(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    @KafkaHandler
    public void onStockReserved(StockReservedEvent event) {
        log.info("Received StockReservedEvent for orderId={}", event.getOrderId());
        orderRepository.findById(event.getOrderId()).ifPresentOrElse(order -> {
            order.setStatus(Order.OrderStatus.PENDING_PAYMENT);
            orderRepository.save(order);
        }, () -> log.warn("StockReservedEvent for unknown orderId={}", event.getOrderId()));
    }

    @KafkaHandler
    public void onStockReservationFailed(StockReservationFailedEvent event) {
        log.info("Received StockReservationFailedEvent for orderId={}: {}",
                event.getOrderId(), event.getReason());
        orderRepository.findById(event.getOrderId()).ifPresentOrElse(order -> {
            order.setStatus(Order.OrderStatus.FAILED);
            order.setFailureReason(event.getReason());
            orderRepository.save(order);
        }, () -> log.warn("StockReservationFailedEvent for unknown orderId={}", event.getOrderId()));
    }

    @KafkaHandler
    public void onInventoryRestore(InventoryRestoreEvent event) {
        log.info("Received InventoryRestoreEvent for orderId={}: restored {} units of productId={}",
                event.getOrderId(), event.getQuantity(), event.getProductId());
        orderRepository.findById(event.getOrderId()).ifPresent(order -> {
            log.info("Saga rollback confirmed for orderId={}: stock compensated successfully", event.getOrderId());
        });
    }

    @KafkaHandler(isDefault = true)
    public void onUnhandled(Object event) {
        log.warn("Received unrecognized event on inventory-events: {}", event);
    }
}
