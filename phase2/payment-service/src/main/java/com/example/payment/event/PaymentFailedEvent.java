package com.example.order.event;

import java.time.Instant;

/**
 * Published by payment-service to "payment-events" when a charge fails.
 * This is the trigger for the saga's COMPENSATING TRANSACTION:
 *   - order-service marks the order FAILED
 *   - inventory-service listens for this SAME event and releases
 *     (restores) the stock it reserved earlier, undoing its part of
 *     the saga.
 * This is the core distributed-rollback demonstration of Phase 2.
 */
public class PaymentFailedEvent {

    private Long orderId;
    private Long productId;
    private Integer quantity;
    private String reason;
    private Instant occurredAt;

    public PaymentFailedEvent() {
    }

    public PaymentFailedEvent(Long orderId, Long productId, Integer quantity, String reason) {
        this.orderId = orderId;
        this.productId = productId;
        this.quantity = quantity;
        this.reason = reason;
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

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }

    public void setOccurredAt(Instant occurredAt) {
        this.occurredAt = occurredAt;
    }
}