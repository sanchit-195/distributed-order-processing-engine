package com.example.order.event;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Published to "order-events" when an order is first created.
 * Kicks off the saga: inventory-service listens for this and attempts
 * to reserve stock.
 */
public class OrderCreatedEvent {

    private Long orderId;
    private Long productId;
    private Integer quantity;
    private BigDecimal totalAmount;
    private Instant occurredAt;

    public OrderCreatedEvent() {
    }

    public OrderCreatedEvent(Long orderId, Long productId, Integer quantity, BigDecimal totalAmount) {
        this.orderId = orderId;
        this.productId = productId;
        this.quantity = quantity;
        this.totalAmount = totalAmount;
        this.occurredAt = Instant.now();
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public Long getProductId() {
        return productId;
    }

    public void setProductId(Long productId) {
        this.productId = productId;
    }

    public Integer getQuantity() {
        return quantity;
    }

    public void setQuantity(Integer quantity) {
        this.quantity = quantity;
    }

    public BigDecimal getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(BigDecimal totalAmount) {
        this.totalAmount = totalAmount;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }

    public void setOccurredAt(Instant occurredAt) {
        this.occurredAt = occurredAt;
    }
}