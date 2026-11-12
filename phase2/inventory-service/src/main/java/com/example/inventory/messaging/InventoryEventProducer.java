package com.example.inventory.messaging;

import com.example.inventory.event.InventoryRestoreEvent;
import com.example.inventory.event.StockReservationFailedEvent;
import com.example.inventory.event.StockReservedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Component
public class InventoryEventProducer {

    private static final Logger log = LoggerFactory.getLogger(InventoryEventProducer.class);
    private static final String TOPIC = "inventory-events";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public InventoryEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT, fallbackExecution = true)
    public void publishStockReserved(StockReservedEvent event) {
        runAfterCommit(() -> {
            String key = String.valueOf(event.getOrderId());
            log.info("Publishing StockReservedEvent for orderId={}", event.getOrderId());
            kafkaTemplate.send(TOPIC, key, event);
        });
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT, fallbackExecution = true)
    public void publishStockReservationFailed(StockReservationFailedEvent event) {
        runAfterCommit(() -> {
            String key = String.valueOf(event.getOrderId());
            log.info("Publishing StockReservationFailedEvent for orderId={}: {}",
                    event.getOrderId(), event.getReason());
            kafkaTemplate.send(TOPIC, key, event);
        });
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT, fallbackExecution = true)
    public void publishInventoryRestore(InventoryRestoreEvent event) {
        runAfterCommit(() -> {
            String key = String.valueOf(event.getOrderId());
            log.info("Publishing InventoryRestoreEvent for orderId={}: restored {} units of productId={}",
                    event.getOrderId(), event.getQuantity(), event.getProductId());
            kafkaTemplate.send(TOPIC, key, event);
        });
    }

    private void runAfterCommit(Runnable action) {
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    action.run();
                }
            });
        } else {
            action.run();
        }
    }
}
