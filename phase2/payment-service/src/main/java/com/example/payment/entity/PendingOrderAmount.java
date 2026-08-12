package com.example.payment.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;

/**
 * payment-service doesn't own price/amount data - order-service computed
 * totalAmount from a caller-supplied unitPrice back when the order was
 * created. StockReservedEvent (the event that actually triggers a charge
 * attempt) doesn't carry that amount, only productId/quantity.
 *
 * To actually know what to charge, payment-service ALSO listens to
 * "order-events" (OrderCreatedEvent) purely to cache totalAmount here,
 * keyed by orderId, until the matching StockReservedEvent arrives later
 * and this gets looked up and used.
 */
@Entity
@Table(name = "pending_order_amounts")
public class PendingOrderAmount {

    @Id
    private Long orderId;

    private BigDecimal totalAmount;

    protected PendingOrderAmount() {
    }

    public PendingOrderAmount(Long orderId, BigDecimal totalAmount) {
        this.orderId = orderId;
        this.totalAmount = totalAmount;
    }

    public Long getOrderId() {
        return orderId;
    }

    public BigDecimal getTotalAmount() {
        return totalAmount;
    }
}