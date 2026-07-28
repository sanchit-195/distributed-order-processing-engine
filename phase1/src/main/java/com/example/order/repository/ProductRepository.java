package com.example.order.repository;

import com.example.order.entity.Product;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;

import java.util.Optional;

public interface ProductRepository extends JpaRepository<Product, Long> {

    // PESSIMISTIC_WRITE tells Hibernate to issue:
    //   SELECT ... FROM products WHERE id = ? FOR UPDATE
    // Any other transaction trying to read this same row with a lock
    // will block until this transaction commits or rolls back.
    // This is what prevents two concurrent orders from both reading
    // stock=1 and both deciding they're allowed to sell the last unit.
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT p FROM Product p WHERE p.id = :id")
    Optional<Product> findByIdForUpdate(Long id);
}
