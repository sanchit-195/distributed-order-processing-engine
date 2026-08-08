package com.example.inventory.repository;

import com.example.inventory.entity.StockReservation;
import org.springframework.data.jpa.repository.JpaRepository;

public interface StockReservationRepository extends JpaRepository<StockReservation, Long> {
    // orderId is the primary key, so JpaRepository.findById(orderId)
    // already gives us the idempotency check we need - no custom
    // query required.
}
