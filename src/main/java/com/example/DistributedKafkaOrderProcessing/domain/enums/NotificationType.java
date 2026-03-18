package com.example.DistributedKafkaOrderProcessing.domain.enums;

public enum NotificationType {
    ORDER_RECEIVED,
    PAYMENT_SUCCESS,
    PAYMENT_FAILED,
    INVENTORY_RESERVED,
    INVENTORY_FAILED,
    SHIPPING_BOOKED,
    SHIPPING_FAILED,
    ORDER_CONFIRMED,
    ORDER_FAILED
}
