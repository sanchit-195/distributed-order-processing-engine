package com.example.order.messaging;

import com.example.order.entity.Order;
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
 * "order-service". Both StockReservedEvent and StockReservationFailedEvent
 * flow through this one topic, so we use @KafkaHandler to dispatch by the
 * deserialized payload's runtime type instead of registering separate
 * listeners on the same group+topic (which would make Kafka split
 * partitions between them instead of both seeing every message).
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

    @KafkaHandler(isDefault = true)
    public void onUnhandled(Object event) {
        log.warn("Received unrecognized event on inventory-events: {}", event);
    }
}