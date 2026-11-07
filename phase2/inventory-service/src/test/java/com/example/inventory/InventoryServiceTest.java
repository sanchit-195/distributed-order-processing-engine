package com.example.inventory;

import com.example.inventory.entity.Product;
import com.example.inventory.entity.StockReservation;
import com.example.inventory.event.InventoryRestoreEvent;
import com.example.inventory.event.StockReservationFailedEvent;
import com.example.inventory.event.StockReservedEvent;
import com.example.inventory.messaging.InventoryEventProducer;
import com.example.inventory.repository.ProductRepository;
import com.example.inventory.repository.StockReservationRepository;
import com.example.inventory.service.InventoryService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@SpringBootTest
class InventoryServiceTest {

    @Autowired
    private InventoryService inventoryService;

    @Autowired
    private ProductRepository productRepository;

    @Autowired
    private StockReservationRepository reservationRepository;

    @MockBean
    private InventoryEventProducer eventProducer;

    private Product testProduct;

    @BeforeEach
    void setUp() {
        reservationRepository.deleteAll();
        testProduct = productRepository.save(new Product("Test Gadget", new BigDecimal("49.99"), 10));
    }

    @Test
    @DisplayName("Happy Path: Reserving stock deducts units and emits StockReservedEvent")
    void testReserveStockSuccess() {
        Long orderId = 1001L;
        int qty = 3;

        inventoryService.reserveStock(orderId, testProduct.getId(), qty, new BigDecimal("149.97"));

        Product updated = productRepository.findById(testProduct.getId()).orElseThrow();
        assertThat(updated.getStock()).isEqualTo(7);

        StockReservation reservation = reservationRepository.findById(orderId).orElseThrow();
        assertThat(reservation.getStatus()).isEqualTo(StockReservation.ReservationStatus.RESERVED);
        assertThat(reservation.getQuantity()).isEqualTo(qty);

        ArgumentCaptor<StockReservedEvent> captor = ArgumentCaptor.forClass(StockReservedEvent.class);
        verify(eventProducer, times(1)).publishStockReserved(captor.capture());
        StockReservedEvent published = captor.getValue();
        assertThat(published.getOrderId()).isEqualTo(orderId);
        assertThat(published.getQuantity()).isEqualTo(qty);
        assertThat(published.getTotalAmount()).isEqualByComparingTo("149.97");
    }

    @Test
    @DisplayName("Out of stock: Rejects reservation and emits StockReservationFailedEvent")
    void testReserveStockInsufficient() {
        Long orderId = 1002L;
        int qty = 20; // more than 10

        inventoryService.reserveStock(orderId, testProduct.getId(), qty, new BigDecimal("999.80"));

        Product updated = productRepository.findById(testProduct.getId()).orElseThrow();
        assertThat(updated.getStock()).isEqualTo(10); // unchanged

        StockReservation reservation = reservationRepository.findById(orderId).orElseThrow();
        assertThat(reservation.getStatus()).isEqualTo(StockReservation.ReservationStatus.REJECTED);

        ArgumentCaptor<StockReservationFailedEvent> captor = ArgumentCaptor.forClass(StockReservationFailedEvent.class);
        verify(eventProducer, times(1)).publishStockReservationFailed(captor.capture());
        assertThat(captor.getValue().getOrderId()).isEqualTo(orderId);
    }

    @Test
    @DisplayName("Compensating Transaction: Payment failure releases stock and emits InventoryRestoreEvent")
    void testCompensatingTransactionReleaseStock() {
        Long orderId = 1003L;
        int qty = 4;

        // Step 1: Stock is reserved initially
        inventoryService.reserveStock(orderId, testProduct.getId(), qty, new BigDecimal("199.96"));
        Product reservedState = productRepository.findById(testProduct.getId()).orElseThrow();
        assertThat(reservedState.getStock()).isEqualTo(6);

        // Step 2: Payment fails, triggering compensating transaction
        inventoryService.releaseStock(orderId);

        // Stock restored back to 10
        Product restoredProduct = productRepository.findById(testProduct.getId()).orElseThrow();
        assertThat(restoredProduct.getStock()).isEqualTo(10);

        StockReservation reservation = reservationRepository.findById(orderId).orElseThrow();
        assertThat(reservation.getStatus()).isEqualTo(StockReservation.ReservationStatus.RELEASED);

        // InventoryRestoreEvent emitted
        ArgumentCaptor<InventoryRestoreEvent> captor = ArgumentCaptor.forClass(InventoryRestoreEvent.class);
        verify(eventProducer, times(1)).publishInventoryRestore(captor.capture());
        InventoryRestoreEvent event = captor.getValue();
        assertThat(event.getOrderId()).isEqualTo(orderId);
        assertThat(event.getProductId()).isEqualTo(testProduct.getId());
        assertThat(event.getQuantity()).isEqualTo(qty);
    }

    @Test
    @DisplayName("Idempotency: Duplicate OrderCreatedEvent does not deduct stock twice")
    void testIdempotentStockReservation() {
        Long orderId = 1004L;
        int qty = 2;

        inventoryService.reserveStock(orderId, testProduct.getId(), qty, new BigDecimal("99.98"));
        inventoryService.reserveStock(orderId, testProduct.getId(), qty, new BigDecimal("99.98"));

        Product product = productRepository.findById(testProduct.getId()).orElseThrow();
        assertThat(product.getStock()).isEqualTo(8); // Only deducted once
    }

    @Test
    @DisplayName("Idempotency: Duplicate releaseStock does not restore stock twice")
    void testIdempotentStockRelease() {
        Long orderId = 1005L;
        int qty = 2;

        inventoryService.reserveStock(orderId, testProduct.getId(), qty, new BigDecimal("99.98"));
        inventoryService.releaseStock(orderId);
        inventoryService.releaseStock(orderId); // second call

        Product product = productRepository.findById(testProduct.getId()).orElseThrow();
        assertThat(product.getStock()).isEqualTo(10); // Restored exactly once
        verify(eventProducer, times(1)).publishInventoryRestore(any());
    }
}

