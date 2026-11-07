package com.example.payment;

import com.example.payment.entity.Payment;
import com.example.payment.entity.PendingOrderAmount;
import com.example.payment.event.PaymentCompletedEvent;
import com.example.payment.event.PaymentFailedEvent;
import com.example.payment.messaging.PaymentEventProducer;
import com.example.payment.repository.PaymentRepository;
import com.example.payment.repository.PendingOrderAmountRepository;
import com.example.payment.service.PaymentService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@SpringBootTest
class PaymentServiceTest {

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private PendingOrderAmountRepository pendingOrderAmountRepository;

    @MockBean
    private PaymentEventProducer eventProducer;

    @BeforeEach
    void setUp() {
        paymentRepository.deleteAll();
        pendingOrderAmountRepository.deleteAll();
    }

    @Test
    @DisplayName("Happy Path: Successful payment saves record and publishes PaymentCompletedEvent")
    void testSuccessfulPayment() {
        PaymentService service = new PaymentService(
                paymentRepository,
                pendingOrderAmountRepository,
                eventProducer,
                List.of(999L), // Only 999 fails deterministically
                false,         // random failure disabled
                0.0
        );

        Long orderId = 2001L;
        Long productId = 1L;
        BigDecimal amount = new BigDecimal("77.97");

        service.processPayment(orderId, productId, 3, amount);

        Payment payment = paymentRepository.findById(orderId).orElseThrow();
        assertThat(payment.getStatus()).isEqualTo(Payment.PaymentStatus.SUCCESS);
        assertThat(payment.getAmount()).isEqualByComparingTo(amount);

        ArgumentCaptor<PaymentCompletedEvent> captor = ArgumentCaptor.forClass(PaymentCompletedEvent.class);
        verify(eventProducer, times(1)).publishPaymentCompleted(captor.capture());
        assertThat(captor.getValue().getOrderId()).isEqualTo(orderId);
        assertThat(captor.getValue().getAmount()).isEqualByComparingTo(amount);
    }

    @Test
    @DisplayName("Deterministic Failure: Configured productId fails payment and publishes PaymentFailedEvent")
    void testDeterministicPaymentFailure() {
        Long failingProductId = 2L;
        PaymentService service = new PaymentService(
                paymentRepository,
                pendingOrderAmountRepository,
                eventProducer,
                List.of(failingProductId),
                false,
                0.0
        );

        Long orderId = 2002L;
        BigDecimal amount = new BigDecimal("89.50");

        service.processPayment(orderId, failingProductId, 1, amount);

        Payment payment = paymentRepository.findById(orderId).orElseThrow();
        assertThat(payment.getStatus()).isEqualTo(Payment.PaymentStatus.FAILED);
        assertThat(payment.getFailureReason()).contains("simulated failure");

        ArgumentCaptor<PaymentFailedEvent> captor = ArgumentCaptor.forClass(PaymentFailedEvent.class);
        verify(eventProducer, times(1)).publishPaymentFailed(captor.capture());
        PaymentFailedEvent event = captor.getValue();
        assertThat(event.getOrderId()).isEqualTo(orderId);
        assertThat(event.getProductId()).isEqualTo(failingProductId);
        assertThat(event.getQuantity()).isEqualTo(1);
    }

    @Test
    @DisplayName("Random Failure: When enabled, triggers payment failure and publishes PaymentFailedEvent")
    void testRandomPaymentFailure() {
        PaymentService service = new PaymentService(
                paymentRepository,
                pendingOrderAmountRepository,
                eventProducer,
                List.of(),
                true, // random failure enabled
                1.0   // 100% failure rate for test predictability
        );

        Long orderId = 2003L;
        Long productId = 1L;
        BigDecimal amount = new BigDecimal("50.00");

        service.processPayment(orderId, productId, 2, amount);

        Payment payment = paymentRepository.findById(orderId).orElseThrow();
        assertThat(payment.getStatus()).isEqualTo(Payment.PaymentStatus.FAILED);
        assertThat(payment.getFailureReason()).contains("random failure");

        verify(eventProducer, times(1)).publishPaymentFailed(any(PaymentFailedEvent.class));
    }

    @Test
    @DisplayName("Idempotency: Redelivered StockReservedEvent does not charge twice")
    void testPaymentIdempotency() {
        PaymentService service = new PaymentService(
                paymentRepository,
                pendingOrderAmountRepository,
                eventProducer,
                List.of(),
                false,
                0.0
        );

        Long orderId = 2004L;
        Long productId = 1L;
        BigDecimal amount = new BigDecimal("25.99");

        service.processPayment(orderId, productId, 1, amount);
        service.processPayment(orderId, productId, 1, amount); // Duplicate delivery

        assertThat(paymentRepository.count()).isEqualTo(1);
        verify(eventProducer, times(1)).publishPaymentCompleted(any());
    }

    @Test
    @DisplayName("Amount fallback: Resolves amount from pending_order_amounts repository")
    void testAmountResolvedFromPendingOrderAmount() {
        PaymentService service = new PaymentService(
                paymentRepository,
                pendingOrderAmountRepository,
                eventProducer,
                List.of(),
                false,
                0.0
        );

        Long orderId = 2005L;
        pendingOrderAmountRepository.save(new PendingOrderAmount(orderId, new BigDecimal("120.00")));

        // Calling with null totalAmount in method - should resolve from pendingOrderAmountRepository
        service.processPayment(orderId, 1L, 2, null);

        Payment payment = paymentRepository.findById(orderId).orElseThrow();
        assertThat(payment.getStatus()).isEqualTo(Payment.PaymentStatus.SUCCESS);
        assertThat(payment.getAmount()).isEqualByComparingTo("120.00");
    }
}

