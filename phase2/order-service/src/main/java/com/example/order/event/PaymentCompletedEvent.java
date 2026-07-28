package com.example.order.event;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Published by payment-service to "payment-events" when a charge succeeds.
 * order-service listens for this to mark the order CONFIRMED - the happy
 * path terminus of the saga.
 */
public class PaymentCompletedEvent {

    private Long orderId;
    private BigDecimal amount;
    private Instant occurredAt;

    public PaymentCompletedEvent() {
    }

    public PaymentCompletedEvent(Long orderId, BigDecimal amount) {
        this.orderId = orderId;
        this.amount = amount;
        this.occurredAt = Instant.now();
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }

    public void setOccurredAt(Instant occurredAt) {
        this.occurredAt = occurredAt;
    }
}