package com.example.inventory.event;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Published by inventory-service to "inventory-events" when stock was
 * successfully deducted. payment-service listens for this to know it's
 * safe to attempt a charge.
 */
public class StockReservedEvent {

    private Long orderId;
    private Long productId;
    private Integer quantity;
    private BigDecimal totalAmount;
    private Instant occurredAt;

    public StockReservedEvent() {
    }

    public StockReservedEvent(Long orderId, Long productId, Integer quantity) {
        this(orderId, productId, quantity, null);
    }

    public StockReservedEvent(Long orderId, Long productId, Integer quantity, BigDecimal totalAmount) {
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
