package com.example.order.messaging;

import com.example.order.entity.Order;
import com.example.order.event.PaymentCompletedEvent;
import com.example.order.event.PaymentFailedEvent;
import com.example.order.event.StockReservationFailedEvent;
import com.example.order.event.StockReservedEvent;
import com.example.order.repository.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * order-service's half of the choreography: it never tells inventory or
 * payment what to do directly. It just reacts to events they publish and
 * updates its own local order state accordingly.
 */
@Component
public class OrderSagaListener {

    private static final Logger log = LoggerFactory.getLogger(OrderSagaListener.class);

    private final OrderRepository orderRepository;

    public OrderSagaListener(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    @KafkaListener(topics = "inventory-events", groupId = "order-service", containerFactory = "stockReservedFactory")
    public void onStockReserved(StockReservedEvent event) {
        log.info("Received StockReservedEvent for orderId={}", event.getOrderId());
        orderRepository.findById(event.getOrderId()).ifPresentOrElse(order -> {
            order.setStatus(Order.OrderStatus.PENDING_PAYMENT);
            orderRepository.save(order);
        }, () -> log.warn("StockReservedEvent for unknown orderId={}", event.getOrderId()));
    }

    @KafkaListener(topics = "inventory-events", groupId = "order-service", containerFactory = "stockReservationFailedFactory")
    public void onStockReservationFailed(StockReservationFailedEvent event) {
        log.info("Received StockReservationFailedEvent for orderId={}: {}", event.getOrderId(), event.getReason());
        orderRepository.findById(event.getOrderId()).ifPresentOrElse(order -> {
            order.setStatus(Order.OrderStatus.FAILED);
            order.setFailureReason(event.getReason());
            orderRepository.save(order);
        }, () -> log.warn("StockReservationFailedEvent for unknown orderId={}", event.getOrderId()));
    }

    @KafkaListener(topics = "payment-events", groupId = "order-service", containerFactory = "paymentCompletedFactory")
    public void onPaymentCompleted(PaymentCompletedEvent event) {
        log.info("Received PaymentCompletedEvent for orderId={}", event.getOrderId());
        orderRepository.findById(event.getOrderId()).ifPresentOrElse(order -> {
            order.setStatus(Order.OrderStatus.CONFIRMED);
            orderRepository.save(order);
        }, () -> log.warn("PaymentCompletedEvent for unknown orderId={}", event.getOrderId()));
    }

    @KafkaListener(topics = "payment-events", groupId = "order-service", containerFactory = "paymentFailedFactory")
    public void onPaymentFailed(PaymentFailedEvent event) {
        // This is the order-service side of the compensating transaction.
        // inventory-service listens to this SAME event independently to
        // release the stock it reserved - the two services don't call
        // each other directly, they just both react to the same broadcast.
        log.info("Received PaymentFailedEvent for orderId={}: {}", event.getOrderId(), event.getReason());
        orderRepository.findById(event.getOrderId()).ifPresentOrElse(order -> {
            order.setStatus(Order.OrderStatus.FAILED);
            order.setFailureReason(event.getReason());
            orderRepository.save(order);
        }, () -> log.warn("PaymentFailedEvent for unknown orderId={}", event.getOrderId()));
    }
}