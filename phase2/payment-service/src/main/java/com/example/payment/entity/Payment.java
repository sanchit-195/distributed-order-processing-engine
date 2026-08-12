package com.example.payment.entity;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;

@Entity
@Table(name = "payments")
public class Payment {

    // orderId doubles as the idempotency key here - same reasoning as
    // inventory-service's StockReservation: if StockReservedEvent gets
    // redelivered by Kafka, we must not charge the same order twice.
    @Id
    private Long orderId;

    @Column(nullable = false)
    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private PaymentStatus status;

    @Column
    private String failureReason;

    @Column(nullable = false)
    private Instant processedAt;

    protected Payment() {
    }

    public Payment(Long orderId, BigDecimal amount, PaymentStatus status, String failureReason) {
        this.orderId = orderId;
        this.amount = amount;
        this.status = status;
        this.failureReason = failureReason;
        this.processedAt = Instant.now();
    }

    public Long getOrderId() {
        return orderId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public PaymentStatus getStatus() {
        return status;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }

    public enum PaymentStatus {
        SUCCESS,
        FAILED
    }
}