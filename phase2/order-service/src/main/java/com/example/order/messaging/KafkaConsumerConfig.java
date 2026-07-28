package com.example.order.messaging;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * order-service consumes events published by inventory-service and
 * payment-service. Those events live in different Java packages
 * (com.example.inventory.event.* / com.example.payment.event.*) than
 * order-service's own copies (com.example.order.event.*), so we can't
 * rely on JsonDeserializer's default behaviour of reading the producer's
 * fully-qualified class name from the __TypeId__ header - that name
 * wouldn't exist as a class here.
 *
 * Instead, both sides agree on short logical type names ("stockReserved",
 * "paymentFailed", etc.) via spring.json.type.mapping. The producer
 * writes the logical name into the header; this consumer looks it up
 * against ITS OWN local classes.
 */
@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-service");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.order.event");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, true);
        props.put(JsonDeserializer.TYPE_MAPPINGS,
                "orderCreated:com.example.order.event.OrderCreatedEvent," +
                "stockReserved:com.example.order.event.StockReservedEvent," +
                "stockReservationFailed:com.example.order.event.StockReservationFailedEvent," +
                "paymentCompleted:com.example.order.event.PaymentCompletedEvent," +
                "paymentFailed:com.example.order.event.PaymentFailedEvent");
        // Start reading only NEW messages if this consumer group has never
        // committed an offset before (first run). Prevents replaying old
        // test messages from earlier manual runs.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }
}