package com.example.DistributedKafkaOrderProcessing.compensation;

import com.example.DistributedKafkaOrderProcessing.domain.enums.OrderStatus;
import com.example.DistributedKafkaOrderProcessing.domain.events.Events;
import com.example.DistributedKafkaOrderProcessing.inventory.InventoryService;
import com.example.DistributedKafkaOrderProcessing.order.IdempotencyService;
import com.example.DistributedKafkaOrderProcessing.order.OrderService;
import com.example.DistributedKafkaOrderProcessing.payment.PaymentService;
import com.example.DistributedKafkaOrderProcessing.shipping.ShippingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

@Component
public class CompensationConsumer {

    private static final Logger log = LoggerFactory.getLogger(CompensationConsumer.class);
    private static final String GROUP = "compensation-service";

    private final OrderService orderService;
    private final PaymentService paymentService;
    private final InventoryService inventoryService;
    private final ShippingService shippingService;
    private final CompensationEventProducer eventProducer;
    private final IdempotencyService idempotencyService;

    public CompensationConsumer(OrderService orderService,
                                PaymentService paymentService,
                                InventoryService inventoryService,
                                ShippingService shippingService,
                                CompensationEventProducer eventProducer,
                                IdempotencyService idempotencyService) {
        this.orderService = orderService;
        this.paymentService = paymentService;
        this.inventoryService = inventoryService;
        this.shippingService = shippingService;
        this.eventProducer = eventProducer;
        this.idempotencyService = idempotencyService;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.compensation}",
            groupId = GROUP,
            concurrency = "3"
    )
    public void handleCompensation(
            @Payload Events.CompensationEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack) {


        log.warn("[compensation-service] CompensationEvent received: " +
                        "orderId={} stage={} refund={} releaseInventory={} partition={}",
                event.orderId(), event.failureStage(),
                event.refundRequired(), event.inventoryReleaseRequired(), partition);

        // ── Idempotency ───────────────────────────────────────────────────────
        if (idempotencyService.alreadyProcessed(event.eventId(), GROUP)) {
            ack.acknowledge();
            return;
        }

//        Mark Order as Compensation in progress
        orderService.updateStatus(event.eventId(), OrderStatus.COMPENSATION_PENDING);
        boolean paymentRefunded=false;
        boolean inventoryReleased=false;

//        Cancel chronologically
        try{

//            Cancel shipment if applicable
            if("SHIPPING".equals(event.failureStage())){
                shippingService.cancelShipment(event.orderId());
                log.info("[compensation-service] Shipment cancellation attempted: orderId={}",
                        event.orderId());
            }

//            Step B :Release inventory(if reserved)
            if (event.inventoryReleaseRequired() && !event.itemsToRelease().isEmpty()) {
                inventoryService.releaseInventory(event.orderId(), event.itemsToRelease());
                inventoryReleased = true;
                log.info("[compensation-service] Inventory released: orderId={}",
                        event.orderId());
            }

//            Step C: Refund payment(if charged)
            if (event.refundRequired() && event.transactionId() != null) {
                paymentRefunded = paymentService.refundPayment(
                        event.orderId(), event.transactionId());
                log.info("[compensation-service] Refund processed: orderId={} refunded={}",
                        event.orderId(), paymentRefunded);
            }
        } catch (Exception ex){

//            Compensation itself failed. alert on-call, store to compensation failures table
            log.error("CRITICAL: Compensation failed for orderId={} stage={}",
                    event.orderId(), event.failureStage(), ex);
        }

//        Mark order as FAILED regardless of the outcome in compensation
        orderService.failOrder(event.orderId(), buildFailureReason(event));
        log.warn("[compensation-service] Order FAILED: orderId={} stage={}",
                event.orderId(), event.failureStage());

//        Publish OrderFailedEvent
        Events.OrderFailedEvent failedEvent= new Events.OrderFailedEvent(
                UUID.randomUUID().toString(),
                event.orderId(),
                event.customerId(),
                event.customerEmail(),
                event.customerPhone(),
                event.totalAmount(),
                event.failureStage(),
                event.failureReason(),
                paymentRefunded,
                inventoryReleased,
                Instant.now()
        );
        eventProducer.publishOrderFailed(failedEvent);
        ack.acknowledge();

        log.warn("[compensation-service] Saga rollback complete: " +
                        "orderId={} refunded={} inventoryReleased={}",
                event.orderId(), paymentRefunded, inventoryReleased);

    }

    private String buildFailureReason(Events.CompensationEvent event) {
        return switch (event.failureStage()) {
            case "PAYMENT"   -> "Payment failed: " + event.failureReason();
            case "INVENTORY" -> "Items out of stock: " + event.failureReason();
            case "SHIPPING"  -> "Shipping unavailable: " + event.failureReason();
            default          -> "Order processing failed: " + event.failureReason();
        };

    }
}
