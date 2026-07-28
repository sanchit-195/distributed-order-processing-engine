package com.example.order.service;

import com.example.order.entity.Order;
import com.example.order.event.OrderCreatedEvent;
import com.example.order.messaging.OrderEventProducer;
import com.example.order.repository.OrderRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.Optional;

@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OrderEventProducer orderEventProducer;

    public OrderService(OrderRepository orderRepository, OrderEventProducer orderEventProducer) {
        this.orderRepository = orderRepository;
        this.orderEventProducer = orderEventProducer;
    }

    /**
     * Unlike Phase 1's placeOrder(), this method does NOT touch stock or
     * payment at all - order-service doesn't own that data anymore. It
     * only creates a local PENDING_STOCK record and publishes an event.
     * The actual outcome (CONFIRMED/FAILED) is decided asynchronously by
     * InventoryEventsListener / PaymentEventsListener reacting to events
     * from the other two services later.
     *
     * NOTE: price is hardcoded here as a placeholder since order-service
     * no longer owns Product data - in a real system this would either
     * be passed in by the caller/UI, or looked up via a synchronous call
     * to inventory-service before the saga starts. Keeping it simple and
     * explicit for Phase 2's purposes rather than papering over it.
     */
    @Transactional
    public Order placeOrder(Long productId, Integer quantity, BigDecimal unitPrice) {
        BigDecimal totalAmount = unitPrice.multiply(BigDecimal.valueOf(quantity));

        Order order = new Order(productId, quantity, totalAmount, Order.OrderStatus.PENDING_STOCK);
        order = orderRepository.save(order);

        OrderCreatedEvent event = new OrderCreatedEvent(order.getId(), productId, quantity, totalAmount);
        orderEventProducer.publishOrderCreated(event);

        return order;
    }

    @Transactional(readOnly = true)
    public Optional<Order> findById(Long id) {
        return orderRepository.findById(id);
    }
}