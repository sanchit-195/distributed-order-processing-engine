package com.example.inventory.service;

import com.example.inventory.entity.Product;
import com.example.inventory.entity.StockReservation;
import com.example.inventory.event.StockReservationFailedEvent;
import com.example.inventory.event.StockReservedEvent;
import com.example.inventory.messaging.InventoryEventProducer;
import com.example.inventory.repository.ProductRepository;
import com.example.inventory.repository.StockReservationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

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
     * transaction - inventory-service's own ACID guarantee for ITS data,
     * same as Phase 1, just scoped to one step of a larger saga now.
     */
    @Transactional
    public void reserveStock(Long orderId, Long productId, Integer quantity) {
        // Idempotency check: if Kafka redelivers this OrderCreatedEvent
        // (consumer restart before offset commit, etc), don't reserve
        // twice for the same order.
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

        eventProducer.publishStockReserved(new StockReservedEvent(orderId, productId, quantity));
    }

    /**
     * The compensating transaction. Reacts to PaymentFailedEvent by
     * restoring the stock that was deducted in reserveStock() above.
     * This is manual, explicit rollback logic - the thing Phase 1 got
     * for free from @Transactional, and Phase 2 has to write by hand
     * because the reservation and the payment attempt happen in two
     * completely separate services with no shared transaction.
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
        if (product != null) {
            product.setStock(product.getStock() + reservation.getQuantity());
            productRepository.save(product);
        } else {
            log.warn("Product id={} no longer exists, cannot restore stock for orderId={}",
                    reservation.getProductId(), orderId);
        }

        reservation.setStatus(StockReservation.ReservationStatus.RELEASED);
        reservationRepository.save(reservation);

        log.info("Released {} units of productId={} for orderId={} after payment failure",
                reservation.getQuantity(), reservation.getProductId(), orderId);
    }
}
