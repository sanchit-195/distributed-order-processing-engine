package com.example.payment.messaging;

import com.example.payment.event.PaymentCompletedEvent;
import com.example.payment.event.PaymentFailedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class PaymentEventProducer {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventProducer.class);
    private static final String TOPIC = "payment-events";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public PaymentEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishPaymentCompleted(PaymentCompletedEvent event) {
        String key = String.valueOf(event.getOrderId());
        log.info("Publishing PaymentCompletedEvent for orderId={}", event.getOrderId());
        kafkaTemplate.send(TOPIC, key, event);
    }

    public void publishPaymentFailed(PaymentFailedEvent event) {
        String key = String.valueOf(event.getOrderId());
        log.info("Publishing PaymentFailedEvent for orderId={}: {}", event.getOrderId(), event.getReason());
        kafkaTemplate.send(TOPIC, key, event);
    }
}