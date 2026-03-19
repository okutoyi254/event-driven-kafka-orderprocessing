package com.example.DistributedKafkaOrderProcessing.domain.events;

import com.example.DistributedKafkaOrderProcessing.domain.Entities;
import com.example.DistributedKafkaOrderProcessing.domain.enums.InventoryStatus;
import com.example.DistributedKafkaOrderProcessing.domain.enums.PaymentStatus;
import com.example.DistributedKafkaOrderProcessing.domain.enums.ShippingStatus;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;


public final class Events {

    private Events() {}

    // ── 1. Order Placed ───────────────────────────────────────────────────────
    // Published by: OrderService
    // Consumed by:  PaymentConsumer, NotificationConsumer

    public record OrderPlacedEvent(
            String eventId,
            String orderId,
            String customerId,
            String customerEmail,
            String customerPhone,
            List<OrderItem> items,
            BigDecimal totalAmount,
            String shippingAddress,
            Instant placedAt
    ) {}

    public record OrderItem(
            String productId,
            String productName,
            String productCategory,  // e.g. POWER_TOOLS, PLUMBING, ELECTRICAL
            int quantity,
            BigDecimal unitPrice,
            BigDecimal subtotal
    ) {}

    // ── 2. Payment Result ─────────────────────────────────────────────────────
    // Published by: PaymentConsumer
    // Consumed by:  InventoryConsumer (on SUCCESS), NotificationConsumer,
    //               CompensationConsumer (on FAILED)

    public record PaymentResultEvent(
            String eventId,
            String orderId,
            String customerId,
            String customerEmail,
            PaymentStatus status,
            String transactionId,       // payment gateway reference, null if failed
            BigDecimal amountCharged,
            String failureReason,       // null if success
            Instant processedAt
    ) {}

    // ── 3. Inventory Result ───────────────────────────────────────────────────
    // Published by: InventoryConsumer
    // Consumed by:  ShippingConsumer (on RESERVED), NotificationConsumer,
    //               CompensationConsumer (on INSUFFICIENT_STOCK)

    public record InventoryResultEvent(
            String eventId,
            String orderId,
            String customerId,
            String customerEmail,
            String transactionId,        // payment ref — passed through for compensation
            InventoryStatus status,
            List<ReservedItem> reservedItems,
            List<String> outOfStockProductIds,
            String failureReason,
            Instant processedAt
    ) {}

    public record ReservedItem(
            String productId,
            String productName,
            int quantityReserved,
            String warehouseLocation
    ) {}

    // ── 4. Shipping Result ────────────────────────────────────────────────────
    // Published by: ShippingConsumer
    // Consumed by:  OrderConsumer (final saga step), NotificationConsumer,
    //               CompensationConsumer (on UNAVAILABLE)

    public record ShippingResultEvent(
            String eventId,
            String orderId,
            String customerId,
            String customerEmail,
            String transactionId,        // payment ref — passed through for compensation
            ShippingStatus status,
            String trackingNumber,       // null if failed
            String carrier,              // e.g. "FedEx", "UPS", null if failed
            LocalDate estimatedDelivery, // null if failed
            String shippingAddress,
            BigDecimal totalAmount,
            List<ReservedItem> reservedItems,
            String failureReason,
            Instant bookedAt
    ) {}

    // ── 5. Order Confirmed (Saga Success) ─────────────────────────────────────
    // Published by: OrderConsumer (after successful ShippingResultEvent)
    // Consumed by:  NotificationConsumer

    public record OrderConfirmedEvent(
            String eventId,
            String orderId,
            String customerId,
            String customerEmail,
            String customerPhone,
            BigDecimal totalAmount,
            String transactionId,
            String trackingNumber,
            String carrier,
            LocalDate estimatedDelivery,
            String shippingAddress,
            List<OrderItem> items,
            Instant confirmedAt
    ) {

    }

    // ── 6. Order Failed (Saga Failure) ────────────────────────────────────────
    // Published by: CompensationConsumer
    // Consumed by:  NotificationConsumer

    public record OrderFailedEvent(
            String eventId,
            String orderId,
            String customerId,
            String customerEmail,
            String customerPhone,
            BigDecimal totalAmount,
            String failureStage,         // "PAYMENT", "INVENTORY", "SHIPPING"
            String failureReason,
            boolean paymentRefunded,
            boolean inventoryReleased,
            Instant failedAt
    ) {}

    // ── 7. Compensation Event (Saga Rollback) ─────────────────────────────────
    // Published by: CompensationConsumer when any saga step fails
    // Consumed by:  PaymentConsumer (refund), InventoryConsumer (release)

    public record CompensationEvent(
            String eventId,
            String orderId,
            String customerId,
            String customerEmail,
            String customerPhone,
            String transactionId,        // needed for refund
            String failureStage,         // which stage triggered compensation
            String failureReason,
            boolean refundRequired,      // was payment already charged?
            boolean inventoryReleaseRequired, // was inventory already reserved?
            List<ReservedItem> itemsToRelease,
            BigDecimal amountToRefund,
            BigDecimal totalAmount,
            Instant triggeredAt
    ) {}
}