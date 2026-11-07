package com.example.payment.event;

import java.time.Instant;

/**
 * Published by inventory-service to "inventory-events" when a compensating
 * transaction releases (restores) reserved stock following a payment failure.
 * This signals to the saga ecosystem that the distributed rollback completed.
 */
public class InventoryRestoreEvent {

    private Long orderId;
    private Long productId;
    private Integer quantity;
    private String reason;
    private Instant occurredAt;

    public InventoryRestoreEvent() {
    }

    public InventoryRestoreEvent(Long orderId, Long productId, Integer quantity, String reason) {
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

