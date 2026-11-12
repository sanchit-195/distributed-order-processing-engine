package com.example.inventory;

import com.example.inventory.event.StockReservedEvent;
import com.example.inventory.messaging.InventoryEventProducer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@SpringBootTest
class InventoryEventProducerTest {

    @Autowired
    private InventoryEventProducer eventProducer;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @MockBean
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Test
    @DisplayName("Kafka event ordering: StockReservedEvent published AFTER database transaction commits")
    void testEventPublishedAfterCommit() {
        TransactionTemplate tx = new TransactionTemplate(transactionManager);
        StockReservedEvent event = new StockReservedEvent(9901L, 1L, 2, new BigDecimal("51.98"));

        tx.execute(status -> {
            eventProducer.publishStockReserved(event);
            // Inside the uncommitted transaction: kafkaTemplate.send should NOT have been called yet
            verify(kafkaTemplate, never()).send(anyString(), anyString(), any());
            return null;
        });

        // After transaction commits: kafkaTemplate.send MUST have been called
        verify(kafkaTemplate, times(1)).send(eq("inventory-events"), eq("9901"), eq(event));
    }

    @Test
    @DisplayName("Kafka event ordering: StockReservedEvent is NEVER published if transaction rolls back")
    void testEventNotPublishedOnRollback() {
        TransactionTemplate tx = new TransactionTemplate(transactionManager);
        StockReservedEvent event = new StockReservedEvent(9902L, 1L, 2, new BigDecimal("51.98"));

        try {
            tx.execute(status -> {
                eventProducer.publishStockReserved(event);
                throw new RuntimeException("Simulated database failure to trigger rollback");
            });
        } catch (RuntimeException ignored) {}

        // After rollback: kafkaTemplate.send must NEVER be called
        verify(kafkaTemplate, never()).send(anyString(), anyString(), any());
    }
}

