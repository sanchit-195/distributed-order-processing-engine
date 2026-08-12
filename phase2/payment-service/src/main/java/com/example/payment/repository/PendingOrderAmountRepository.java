package com.example.payment.repository;

import com.example.payment.entity.PendingOrderAmount;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PendingOrderAmountRepository extends JpaRepository<PendingOrderAmount, Long> {
}