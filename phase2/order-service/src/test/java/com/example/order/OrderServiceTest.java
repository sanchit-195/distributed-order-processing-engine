package com.example.order;

import com.example.order.entity.Order;
import com.example.order.event.*;
import com.example.order.messaging.InventoryEventsListener;
import com.example.order.messaging.OrderEventProducer;
import com.example.order.messaging.PaymentEventsListener;
import com.example.order.repository.OrderRepository;
import com.example.order.service.OrderService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@SpringBootTest
class OrderServiceTest {

    @Autowired
    private OrderService orderService;

    @Autowired
    private OrderRepository orderRepository;

    @MockBean
    private OrderEventProducer eventProducer;

    @Autowired
    private InventoryEventsListener inventoryEventsListener;

    @Autowired
    private PaymentEventsListener paymentEventsListener;

    @BeforeEach
    void setUp() {
        orderRepository.deleteAll();
    }

    @Test
    @DisplayName("Saga Initiator: Placing order creates PENDING_STOCK record and publishes OrderCreatedEvent")
    void testPlaceOrder() {
        Long productId = 1L;
        int qty = 2;
        BigDecimal unitPrice = new BigDecimal("25.99");

        Order order = orderService.placeOrder(productId, qty, unitPrice);

        assertThat(order.getId()).isNotNull();
        assertThat(order.getStatus()).isEqualTo(Order.OrderStatus.PENDING_STOCK);
        assertThat(order.getTotalAmount()).isEqualByComparingTo("51.98");

        ArgumentCaptor<OrderCreatedEvent> captor = ArgumentCaptor.forClass(OrderCreatedEvent.class);
        verify(eventProducer, times(1)).publishOrderCreated(captor.capture());
        OrderCreatedEvent published = captor.getValue();
        assertThat(published.getOrderId()).isEqualTo(order.getId());
        assertThat(published.getProductId()).isEqualTo(productId);
        assertThat(published.getQuantity()).isEqualTo(qty);
        assertThat(published.getTotalAmount()).isEqualByComparingTo("51.98");
    }

    @Test
    @DisplayName("Saga Happy Path: StockReserved moves order to PENDING_PAYMENT, then PaymentCompleted confirms order")
    void testSagaHappyPathTransitions() {
        Order order = orderRepository.save(
                new Order(1L, 2, new BigDecimal("50.00"), Order.OrderStatus.PENDING_STOCK));

        // Step 1: Stock is reserved by Inventory Service
        inventoryEventsListener.onStockReserved(new StockReservedEvent(order.getId(), 1L, 2, new BigDecimal("50.00")));

        Order afterStock = orderRepository.findById(order.getId()).orElseThrow();
        assertThat(afterStock.getStatus()).isEqualTo(Order.OrderStatus.PENDING_PAYMENT);

        // Step 2: Payment completes
        paymentEventsListener.onPaymentCompleted(new PaymentCompletedEvent(order.getId(), new BigDecimal("50.00")));

        Order finalOrder = orderRepository.findById(order.getId()).orElseThrow();
        assertThat(finalOrder.getStatus()).isEqualTo(Order.OrderStatus.CONFIRMED);
    }

    @Test
    @DisplayName("Saga Failure Path 1: StockReservationFailed transitions order to FAILED")
    void testStockReservationFailed() {
        Order order = orderRepository.save(
                new Order(1L, 100, new BigDecimal("2500.00"), Order.OrderStatus.PENDING_STOCK));

        inventoryEventsListener.onStockReservationFailed(
                new StockReservationFailedEvent(order.getId(), 1L, "Insufficient stock"));

        Order updated = orderRepository.findById(order.getId()).orElseThrow();
        assertThat(updated.getStatus()).isEqualTo(Order.OrderStatus.FAILED);
        assertThat(updated.getFailureReason()).isEqualTo("Insufficient stock");
    }

    @Test
    @DisplayName("Saga Failure Path 2: Payment failure marks order FAILED and receives InventoryRestore confirmation")
    void testPaymentFailureAndRollback() {
        Order order = orderRepository.save(
                new Order(2L, 1, new BigDecimal("89.50"), Order.OrderStatus.PENDING_STOCK));

        // Stock reserved
        inventoryEventsListener.onStockReserved(new StockReservedEvent(order.getId(), 2L, 1, new BigDecimal("89.50")));

        // Payment failed
        paymentEventsListener.onPaymentFailed(
                new PaymentFailedEvent(order.getId(), 2L, 1, "Card declined"));

        Order failedOrder = orderRepository.findById(order.getId()).orElseThrow();
        assertThat(failedOrder.getStatus()).isEqualTo(Order.OrderStatus.FAILED);
        assertThat(failedOrder.getFailureReason()).isEqualTo("Card declined");

        // Compensating transaction event received from Inventory
        inventoryEventsListener.onInventoryRestore(
                new InventoryRestoreEvent(order.getId(), 2L, 1, "Stock restored after payment failure"));
    }
}

