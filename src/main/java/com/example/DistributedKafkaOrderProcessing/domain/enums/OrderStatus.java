package com.example.DistributedKafkaOrderProcessing.domain.enums;

public enum OrderStatus {

    PENDING,
    PAYMENT_PROCESSING,
    PAYMENT_FAILED,
    INVENTORY_CHECKING,
    INVENTORY_FAILED,
    SHIPPING_BOOKING,
    SHIPPING_FAILED,
    CONFIRMED,
    CANCELLED,
    COMPENSATION_PENDING

}
