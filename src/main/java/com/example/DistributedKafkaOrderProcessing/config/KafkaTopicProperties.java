package com.example.DistributedKafkaOrderProcessing.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix ="app.kafka.topics")
public record KafkaTopicProperties (
        String orderPlaced,
        String paymentResult,
        String inventoryResult,
        String shippingResult,
        String orderConfirmed,
        String orderFailed,
        String compensation,

        // DLTs
        String orderPlacedDlt,
        String paymentResultDlt,
        String inventoryResultDlt,
        String shippingResultDlt
){}
