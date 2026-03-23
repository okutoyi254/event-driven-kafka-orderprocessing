package com.example.DistributedKafkaOrderProcessing.notification;

import com.example.DistributedKafkaOrderProcessing.domain.enums.InventoryStatus;
import com.example.DistributedKafkaOrderProcessing.domain.enums.PaymentStatus;
import com.example.DistributedKafkaOrderProcessing.domain.enums.ShippingStatus;
import com.example.DistributedKafkaOrderProcessing.domain.events.Events;
import com.example.DistributedKafkaOrderProcessing.order.IdempotencyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class NotificationConsumer {

    private static  final Logger log = LoggerFactory.getLogger(NotificationConsumer.class);
    private static final String GROUP="notification-service";

    private final NotificationService notificationService;
    private final IdempotencyService idempotencyService;

    public NotificationConsumer(NotificationService notificationService, IdempotencyService idempotencyService) {
        this.notificationService = notificationService;
        this.idempotencyService = idempotencyService;
    }

    // ── order.placed → immediate acknowledgement to customer ─────────────────

    @KafkaListener(
            topics = "${app.kafka.topics.order-placed}",
            groupId = GROUP,
            concurrency = "3"
    )
    public void handleOrderPlaced(
            @Payload Events.OrderPlacedEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            Acknowledgment ack) {

        log.info("[notification-service] OrderPlacedEvent: orderId={}", event.orderId());
        try {
            if (!idempotencyService.alreadyProcessed(event.eventId(), GROUP + "-placed")) {
                notificationService.notifyOrderReceived(event);
            }
        } catch (Exception e) {
            // Log but never rethrow — notification failure must not affect saga
            log.error("[notification-service] Failed to notify ORDER_RECEIVED: orderId={}",
                    event.orderId(), e);
        } finally {
            ack.acknowledge();  // always ack regardless of notification outcome
        }
    }

    @KafkaListener(
            topics = "${app.kafka.tpoics.payment-result}",
            groupId = GROUP,
            concurrency = "3"
    )
    public void handlePaymentResult(
            @Payload Events.PaymentResultEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            Acknowledgment ack) {

        log.info("[notification-service] PaymentResultEvent: orderId={} status={}",
                event.orderId(),event.status());

        try{

            String idempotencyKey= event.orderId()+"-notification-payment";
            if(!idempotencyService.alreadyProcessed(idempotencyKey,GROUP)){
                if(event.status() == PaymentStatus.SUCCESS){
                    notificationService.notifyPaymentSuccess(event);
                }
                else{
                    notificationService.notifyPaymentFailed(event);
                }
            }
        } catch( Exception ex){
            log.error("notification-service] Failed to notify PAYMENT result: orderId={}",
                    event.orderId(), ex);
        }
        finally {
            ack.acknowledge();
        }
    }
    // ── inventory.result → stock confirmed or out-of-stock notification ───────

    @KafkaListener(
            topics = "${app.kafka.topics.inventory-result}",
            groupId = GROUP,
            concurrency = "3"
    )
    public void handleInventoryResult(
            @Payload Events.InventoryResultEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            Acknowledgment ack) {

        log.info("[notification-service] InventoryResultEvent: orderId={} status={}",
                event.orderId(), event.status());
        try {
            String idempotencyKey = event.eventId() + "-notification-inventory";
            if (!idempotencyService.alreadyProcessed(idempotencyKey, GROUP)) {
                if (event.status() == InventoryStatus.RESERVED) {
                    notificationService.notifyInventoryReserved(event);
                } else {
                    notificationService.notifyInventoryFailed(event);
                }
            }
        } catch (Exception e) {
            log.error("[notification-service] Failed to notify INVENTORY result: orderId={}",
                    event.orderId(), e);
        } finally {
            ack.acknowledge();
        }
    }

    // ── shipping.result → delivery booked notification ────────────────────────

    @KafkaListener(
            topics = "${app.kafka.topics.shipping-result}",
            groupId = GROUP,
            concurrency = "3"
    )
    public void handleShippingResult(
            @Payload Events.ShippingResultEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            Acknowledgment ack) {

        log.info("[notification-service] ShippingResultEvent: orderId={} status={}",
                event.orderId(), event.status());
        try {
            String idempotencyKey = event.eventId() + "-notification-shipping";
            if (!idempotencyService.alreadyProcessed(idempotencyKey, GROUP)) {
                // Only notify on success — failure is handled via order.failed
                if (event.status() == ShippingStatus.BOOKED) {
                    notificationService.notifyShippingBooked(event);
                }
            }
        } catch (Exception e) {
            log.error("[notification-service] Failed to notify SHIPPING result: orderId={}",
                    event.orderId(), e);
        } finally {
            ack.acknowledge();
        }
    }

    // ── order.confirmed → full success email with tracking details ───────────

    @KafkaListener(
            topics = "${app.kafka.topics.order-confirmed}",
            groupId = GROUP,
            concurrency = "3"
    )
    public void handleOrderConfirmed(
            @Payload Events.OrderConfirmedEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            Acknowledgment ack) {

        log.info("[notification-service] OrderConfirmedEvent: orderId={}",
                event.orderId());
        try {
            String idempotencyKey = event.eventId() + "-notification-confirmed";
            if (!idempotencyService.alreadyProcessed(idempotencyKey, GROUP)) {
                notificationService.notifyOrderConfirmed(event);
            }
        } catch (Exception e) {
            log.error("[notification-service] Failed to notify ORDER_CONFIRMED: orderId={}",
                    event.orderId(), e);
        } finally {
            ack.acknowledge();
        }
    }
    // ── order.failed → failure email with refund details ─────────────────────

    @KafkaListener(
            topics = "${app.kafka.topics.order-failed}",
            groupId = GROUP,
            concurrency = "3"
    )
    public void handleOrderFailed(
            @Payload Events.OrderFailedEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            Acknowledgment ack) {

        log.info("[notification-service] OrderFailedEvent: orderId={} stage={}",
                event.orderId(), event.failureStage());
        try {
            String idempotencyKey = event.eventId() + "-notification-failed";
            if (!idempotencyService.alreadyProcessed(idempotencyKey, GROUP)) {
                notificationService.notifyOrderFailed(event);
            }
        } catch (Exception e) {
            log.error("[notification-service] Failed to notify ORDER_FAILED: orderId={}",
                    event.orderId(), e);
        } finally {
            ack.acknowledge();
        }
    }
}
