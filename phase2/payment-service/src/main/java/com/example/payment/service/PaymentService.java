package com.example.payment.service;

import com.example.payment.entity.Payment;
import com.example.payment.entity.PendingOrderAmount;
import com.example.payment.event.PaymentCompletedEvent;
import com.example.payment.event.PaymentFailedEvent;
import com.example.payment.messaging.PaymentEventProducer;
import com.example.payment.repository.PaymentRepository;
import com.example.payment.repository.PendingOrderAmountRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentRepository paymentRepository;
    private final PendingOrderAmountRepository pendingOrderAmountRepository;
    private final PaymentEventProducer eventProducer;

    // Failure injection: any order for a productId in this list always
    // fails payment, deterministically. Configured via:
    //   payment.failing-product-ids=2
    private final List<Long> failingProductIds;

    // Random failure injection: can be enabled to simulate unpredictable payment gateway failures
    private final boolean randomFailureEnabled;
    private final double randomFailureRate;

    public PaymentService(PaymentRepository paymentRepository,
                          PendingOrderAmountRepository pendingOrderAmountRepository,
                          PaymentEventProducer eventProducer,
                          @Value("${payment.failing-product-ids:}") List<Long> failingProductIds,
                          @Value("${payment.random-failure-enabled:false}") boolean randomFailureEnabled,
                          @Value("${payment.random-failure-rate:0.3}") double randomFailureRate) {
        this.paymentRepository = paymentRepository;
        this.pendingOrderAmountRepository = pendingOrderAmountRepository;
        this.eventProducer = eventProducer;
        this.failingProductIds = failingProductIds != null ? failingProductIds : Collections.emptyList();
        this.randomFailureEnabled = randomFailureEnabled;
        this.randomFailureRate = randomFailureRate;
    }

    @Transactional
    public void processPayment(Long orderId, Long productId, Integer quantity) {
        processPayment(orderId, productId, quantity, null);
    }

    @Transactional
    public void processPayment(Long orderId, Long productId, Integer quantity, BigDecimal totalAmount) {
        if (paymentRepository.existsById(orderId)) {
            log.info("Payment for orderId={} already processed, skipping duplicate delivery", orderId);
            return;
        }

        BigDecimal amount = pendingOrderAmountRepository.findById(orderId)
                .map(PendingOrderAmount::getTotalAmount)
                .orElse(null);

        // Fall back to event's totalAmount if PendingOrderAmount cache hasn't arrived yet
        if (amount == null && totalAmount != null) {
            log.info("Using totalAmount={} from StockReservedEvent for orderId={}", totalAmount, orderId);
            amount = totalAmount;
            pendingOrderAmountRepository.save(new PendingOrderAmount(orderId, totalAmount));
        }

        // If still null, wait briefly and retry in case OrderCreatedEvent is committing on another thread
        if (amount == null) {
            for (int i = 0; i < 5; i++) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
                amount = pendingOrderAmountRepository.findById(orderId)
                        .map(PendingOrderAmount::getTotalAmount)
                        .orElse(null);
                if (amount != null) {
                    break;
                }
            }
        }

        if (amount == null) {
            log.warn("No cached amount found for orderId={}, failing payment", orderId);
            failPayment(orderId, productId, quantity, null, "Order amount not found");
            return;
        }

        // 1. Check deterministic failure injection
        if (failingProductIds.contains(productId)) {
            log.info("productId={} is configured to always fail payment (orderId={})", productId, orderId);
            failPayment(orderId, productId, quantity, amount, "Payment declined (simulated failure for productId=" + productId + ")");
            return;
        }

        // 2. Check random failure injection
        if (randomFailureEnabled && ThreadLocalRandom.current().nextDouble() < randomFailureRate) {
            log.info("Payment randomly failed for orderId={}, productId={}", orderId, productId);
            failPayment(orderId, productId, quantity, amount, "Payment declined (random failure simulation)");
            return;
        }

        Payment payment = new Payment(orderId, amount, Payment.PaymentStatus.SUCCESS, null);
        paymentRepository.save(payment);

        eventProducer.publishPaymentCompleted(new PaymentCompletedEvent(orderId, amount));
        log.info("Payment SUCCEEDED for orderId={}, amount={}", orderId, amount);
    }

    private void failPayment(Long orderId, Long productId, Integer quantity, BigDecimal amount, String reason) {
        Payment payment = new Payment(
                orderId, amount != null ? amount : BigDecimal.ZERO, Payment.PaymentStatus.FAILED, reason);
        paymentRepository.save(payment);

        eventProducer.publishPaymentFailed(new PaymentFailedEvent(orderId, productId, quantity, reason));
        log.info("Payment FAILED for orderId={}: {}", orderId, reason);
    }
}
