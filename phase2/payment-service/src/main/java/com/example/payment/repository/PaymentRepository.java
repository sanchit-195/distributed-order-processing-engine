package com.example.payment.repository;

import com.example.payment.entity.Payment;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PaymentRepository extends JpaRepository<Payment, Long> {
    // orderId is the primary key, so findById(orderId) already gives us
    // the idempotency check - no custom query needed.
}