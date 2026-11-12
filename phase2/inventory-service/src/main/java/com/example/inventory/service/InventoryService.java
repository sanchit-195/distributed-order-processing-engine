package com.example.inventory.service;

import com.example.inventory.entity.Product;
import com.example.inventory.entity.StockReservation;
import com.example.inventory.event.InventoryRestoreEvent;
import com.example.inventory.event.StockReservationFailedEvent;
import com.example.inventory.event.StockReservedEvent;
import com.example.inventory.messaging.InventoryEventProducer;
import com.example.inventory.repository.ProductRepository;
import com.example.inventory.repository.StockReservationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;

@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);

    private final ProductRepository productRepository;
    private final StockReservationRepository reservationRepository;
    private final InventoryEventProducer eventProducer;

    public InventoryService(ProductRepository productRepository,
                             StockReservationRepository reservationRepository,
                             InventoryEventProducer eventProducer) {
        this.productRepository = productRepository;
        this.reservationRepository = reservationRepository;
        this.eventProducer = eventProducer;
    }

    /**
     * Reacts to OrderCreatedEvent. Locks the product row, checks stock,
     * and either reserves it (deduct + record RESERVED) or rejects it
     * (record REJECTED, nothing to deduct). Everything here is one local
     * transaction - inventory-service's own ACID guarantee for ITS data.
     * Events are published AFTER transaction commits.
     */
    @Transactional
    public void reserveStock(Long orderId, Long productId, Integer quantity) {
        reserveStock(orderId, productId, quantity, null);
    }

    @Transactional
    public void reserveStock(Long orderId, Long productId, Integer quantity, BigDecimal totalAmount) {
        // Idempotency check: if Kafka redelivers this OrderCreatedEvent,
        // don't reserve twice for the same order.
        if (reservationRepository.findById(orderId).isPresent()) {
            log.info("Reservation for orderId={} already exists, skipping duplicate delivery", orderId);
            return;
        }

        Product product = productRepository.findByIdForUpdate(productId).orElse(null);

        if (product == null) {
            log.warn("No product with id={} for orderId={}", productId, orderId);
            reservationRepository.save(new StockReservation(
                    orderId, productId, quantity, StockReservation.ReservationStatus.REJECTED));
            eventProducer.publishStockReservationFailed(new StockReservationFailedEvent(
                    orderId, productId, "No product with id " + productId));
            return;
        }

        if (product.getStock() < quantity) {
            log.info("Insufficient stock for productId={} (have {}, need {}), orderId={}",
                    productId, product.getStock(), quantity, orderId);
            reservationRepository.save(new StockReservation(
                    orderId, productId, quantity, StockReservation.ReservationStatus.REJECTED));
            eventProducer.publishStockReservationFailed(new StockReservationFailedEvent(
                    orderId, productId,
                    "Only " + product.getStock() + " units left for product " + productId));
            return;
        }

        product.setStock(product.getStock() - quantity);
        productRepository.save(product);

        reservationRepository.save(new StockReservation(
                orderId, productId, quantity, StockReservation.ReservationStatus.RESERVED));

        // Event will be published to Kafka only AFTER transaction commits
        eventProducer.publishStockReserved(new StockReservedEvent(orderId, productId, quantity, totalAmount));
    }

    /**
     * The compensating transaction. Reacts to PaymentFailedEvent by
     * restoring the stock that was deducted in reserveStock() above
     * and emitting InventoryRestoreEvent so the saga ecosystem is informed
     * that stock rollback succeeded.
     * Guaranteed atomic: status is only marked RELEASED if stock is restored.
     */
    @Transactional
    public void releaseStock(Long orderId) {
        StockReservation reservation = reservationRepository.findById(orderId).orElse(null);

        if (reservation == null) {
            // We never reserved anything for this order (e.g. stock
            // reservation itself had already failed), so there's
            // nothing to compensate.
            log.info("No reservation found for orderId={}, nothing to release", orderId);
            return;
        }

        if (reservation.getStatus() != StockReservation.ReservationStatus.RESERVED) {
            // Already RELEASED (duplicate PaymentFailedEvent delivery)
            // or was REJECTED to begin with (never deducted stock).
            log.info("Reservation for orderId={} is {}, skipping release", orderId, reservation.getStatus());
            return;
        }

        Product product = productRepository.findByIdForUpdate(reservation.getProductId()).orElse(null);
        if (product == null) {
            log.error("Product id={} no longer exists, cannot restore stock for orderId={}",
                    reservation.getProductId(), orderId);
            return;
        }

        product.setStock(product.getStock() + reservation.getQuantity());
        productRepository.save(product);

        reservation.setStatus(StockReservation.ReservationStatus.RELEASED);
        reservationRepository.save(reservation);

        log.info("Released {} units of productId={} for orderId={} after payment failure",
                reservation.getQuantity(), reservation.getProductId(), orderId);

        // Emit InventoryRestore event as required by saga compensating transaction, AFTER commit
        eventProducer.publishInventoryRestore(new InventoryRestoreEvent(
                orderId,
                reservation.getProductId(),
                reservation.getQuantity(),
                "Stock restored after payment failure"
        ));
    }
}
