package com.example.order.event;

import java.time.Instant;

/**
 * Published by inventory-service to "inventory-events" when there isn't
 * enough stock. order-service listens for this and marks the order
 * FAILED directly - no compensation needed since payment was never touched.
 */
public class StockReservationFailedEvent {

    private Long orderId;
    private Long productId;
    private String reason;
    private Instant occurredAt;

    public StockReservationFailedEvent() {
    }

    public StockReservationFailedEvent(Long orderId, Long productId, String reason) {
        this.orderId = orderId;
        this.productId = productId;
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