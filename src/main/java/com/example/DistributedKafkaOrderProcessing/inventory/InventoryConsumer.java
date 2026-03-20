package com.example.DistributedKafkaOrderProcessing.inventory;

import com.example.DistributedKafkaOrderProcessing.domain.enums.InventoryStatus;
import com.example.DistributedKafkaOrderProcessing.domain.enums.OrderStatus;
import com.example.DistributedKafkaOrderProcessing.domain.enums.PaymentStatus;
import com.example.DistributedKafkaOrderProcessing.domain.events.Events;
import com.example.DistributedKafkaOrderProcessing.order.IdempotencyService;
import com.example.DistributedKafkaOrderProcessing.order.OrderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Component
public class InventoryConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryConsumer.class);
    private static final String GROUP = "inventory-service";

    private final InventoryService inventoryService;
    private final InventoryEventProducer eventProducer;
    private final OrderService orderService;
    private final IdempotencyService idempotencyService;

    public InventoryConsumer(InventoryService inventoryService, InventoryEventProducer eventProducer, OrderService orderService, IdempotencyService idempotencyService) {
        this.inventoryService = inventoryService;
        this.eventProducer = eventProducer;
        this.orderService = orderService;
        this.idempotencyService = idempotencyService;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.payment-result}",
            groupId = GROUP,
            concurrency = "3"
    )
    public void handlePaymentResult(
            @Payload Events.PaymentResultEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack
            ){

        log.info("[inventory-service] PaymentResultEvent: orderId={} status={} partition={}",
                event.orderId(),event.status(),partition);

//        --Only succeed when payment is successful
        if(event.status() != PaymentStatus.SUCCESS){
            log.info("[inventory-service] Skipping - payment not successful: orderId={}",
                    event.orderId());
            ack.acknowledge();
            return;
        }

        if(idempotencyService.alreadyProcessed(event.eventId(),GROUP)){
            ack.acknowledge();
            return;
        }

//        Update order status
        orderService.updateStatus(event.orderId(), OrderStatus.INVENTORY_CHECKING);

//        Reserve inventory
        var order = orderService.getOrder(event.orderId());
        List<Events.OrderItem> orderItems = order.getItems().stream()
                .map(i->new Events.OrderItem(i.getProductId(),i.getProductName(),
                        i.getProductCategory(),i.getQuantity(),i.getUnitPrice(),i.getSubtotal()))
                .toList();

        Events.InventoryResultEvent result= inventoryService.reserveInventory(event,orderItems);

        // ── Handle result ─────────────────────────────────────────────────────
        if (result.status() == InventoryStatus.RESERVED) {

            // ── SUCCESS PATH ──────────────────────────────────────────────────
            orderService.updateStatus(event.orderId(), OrderStatus.SHIPPING_BOOKING);
            eventProducer.publishInventoryResult(result);
            log.info("✅ [inventory-service] Inventory reserved for orderId={}", event.orderId());

        } else {

            //  FAILURE PATH
            // Stock insufficient — must refund the payment and fail the order.
            // Payment was already charged (step 1 succeeded), so refundRequired=true.
            orderService.updateStatus(event.orderId(), OrderStatus.INVENTORY_FAILED);
            eventProducer.publishInventoryResult(result);

            Events.CompensationEvent compensation = new Events.CompensationEvent(
                    UUID.randomUUID().toString(),
                    event.orderId(),
                    event.customerId(),
                    event.customerEmail(),
                    null,
                    event.transactionId(),
                    "INVENTORY",
                    result.failureReason(),
                    true,                       // refundRequired: payment WAS charged
                    false,                      // no inventory reserved, nothing to release
                    List.of(),
                    event.amountCharged(),
                    event.amountCharged(),
                    Instant.now()
            );
            eventProducer.publishCompensation(compensation);

            log.warn("⚠️  [inventory-service] Insufficient stock, compensation triggered: orderId={}",
                    event.orderId());
        }

        ack.acknowledge();
    }
    }

