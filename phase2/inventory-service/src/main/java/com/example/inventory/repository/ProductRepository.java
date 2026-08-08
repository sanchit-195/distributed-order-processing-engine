package com.example.inventory.repository;

import com.example.inventory.entity.Product;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;

import java.util.Optional;

public interface ProductRepository extends JpaRepository<Product, Long> {

    // SELECT ... FOR UPDATE - same pattern as Phase 1's ProductRepository.
    // Locks the row so two concurrent OrderCreatedEvent consumers can't
    // both read the same stock count and both decide they can fulfill it.
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT p FROM Product p WHERE p.id = :id")
    Optional<Product> findByIdForUpdate(Long id);
}
