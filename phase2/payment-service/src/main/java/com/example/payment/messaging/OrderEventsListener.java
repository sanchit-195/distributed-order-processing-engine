package com.example.payment.messaging;

import com.example.payment.entity.PendingOrderAmount;
import com.example.payment.event.OrderCreatedEvent;
import com.example.payment.repository.PendingOrderAmountRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Only one event type flows through "order-events" today, so a plain
 * typed @KafkaListener is enough here (unlike inventory-events /
 * payment-events, which need @KafkaHandler dispatch).
 *
 * This listener does NOT trigger any payment logic - it only caches
 * totalAmount so it's available later when StockReservedEvent arrives
 * and InventoryEventsListener needs to know what to charge.
 */
@Component
public class OrderEventsListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventsListener.class);

    private final PendingOrderAmountRepository pendingOrderAmountRepository;

    public OrderEventsListener(PendingOrderAmountRepository pendingOrderAmountRepository) {
        this.pendingOrderAmountRepository = pendingOrderAmountRepository;
    }

    @KafkaListener(topics = "order-events", groupId = "payment-service")
    public void onOrderCreated(OrderCreatedEvent event) {
        if (pendingOrderAmountRepository.existsById(event.getOrderId())) {
            log.info("Amount already cached for orderId={}, skipping duplicate delivery", event.getOrderId());
            return;
        }
        log.info("Caching totalAmount={} for orderId={}", event.getTotalAmount(), event.getOrderId());
        pendingOrderAmountRepository.save(
                new PendingOrderAmount(event.getOrderId(), event.getTotalAmount()));
    }
}