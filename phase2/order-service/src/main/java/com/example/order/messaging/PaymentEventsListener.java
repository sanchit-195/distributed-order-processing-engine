package com.example.order.messaging;

import com.example.order.entity.Order;
import com.example.order.event.PaymentCompletedEvent;
import com.example.order.event.PaymentFailedEvent;
import com.example.order.repository.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * One listener container subscribed to "payment-events" as group
 * "order-service". PaymentFailedEvent here is the trigger for the saga's
 * compensating transaction - inventory-service listens to this SAME
 * broadcast independently to release stock, without order-service ever
 * calling it directly.
 */
@Component
@KafkaListener(topics = "payment-events", groupId = "order-service")
public class PaymentEventsListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventsListener.class);

    private final OrderRepository orderRepository;

    public PaymentEventsListener(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    @KafkaHandler
    public void onPaymentCompleted(PaymentCompletedEvent event) {
        log.info("Received PaymentCompletedEvent for orderId={}", event.getOrderId());
        orderRepository.findById(event.getOrderId()).ifPresentOrElse(order -> {
            order.setStatus(Order.OrderStatus.CONFIRMED);
            orderRepository.save(order);
        }, () -> log.warn("PaymentCompletedEvent for unknown orderId={}", event.getOrderId()));
    }

    @KafkaHandler
    public void onPaymentFailed(PaymentFailedEvent event) {
        log.info("Received PaymentFailedEvent for orderId={}: {}", event.getOrderId(), event.getReason());
        orderRepository.findById(event.getOrderId()).ifPresentOrElse(order -> {
            order.setStatus(Order.OrderStatus.FAILED);
            order.setFailureReason(event.getReason());
            orderRepository.save(order);
        }, () -> log.warn("PaymentFailedEvent for unknown orderId={}", event.getOrderId()));
    }

    @KafkaHandler(isDefault = true)
    public void onUnhandled(Object event) {
        log.warn("Received unrecognized event on payment-events: {}", event);
    }
}