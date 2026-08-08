package com.example.inventory.entity;

import jakarta.persistence.*;
import java.time.Instant;

/**
 * Kafka guarantees at-least-once delivery, meaning OrderCreatedEvent or
 * PaymentFailedEvent for the same orderId COULD arrive twice (consumer
 * restart before offset commit, broker retry, etc). Without tracking
 * what's already been done per order, a redelivered OrderCreatedEvent
 * would deduct stock twice for one order, and a redelivered
 * PaymentFailedEvent would restore stock twice.
 *
 * This table makes reserve/release idempotent: before acting, we check
 * whether we've already recorded a reservation for this orderId.
 */
@Entity
@Table(name = "stock_reservations")
public class StockReservation {

    @Id
    private Long orderId;

    @Column(nullable = false)
    private Long productId;

    @Column(nullable = false)
    private Integer quantity;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ReservationStatus status;

    @Column(nullable = false)
    private Instant createdAt;

    @Column
    private Instant updatedAt;

    protected StockReservation() {
    }

    public StockReservation(Long orderId, Long productId, Integer quantity, ReservationStatus status) {
        this.orderId = orderId;
        this.productId = productId;
        this.quantity = quantity;
        this.status = status;
        this.createdAt = Instant.now();
    }

    public Long getOrderId() {
        return orderId;
    }

    public Long getProductId() {
        return productId;
    }

    public Integer getQuantity() {
        return quantity;
    }

    public ReservationStatus getStatus() {
        return status;
    }

    public void setStatus(ReservationStatus status) {
        this.status = status;
        this.updatedAt = Instant.now();
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public enum ReservationStatus {
        RESERVED,   // stock was deducted, waiting on payment outcome
        RELEASED,   // payment failed, stock was restored (compensated)
        REJECTED    // never had enough stock to begin with; nothing to release
    }
}
