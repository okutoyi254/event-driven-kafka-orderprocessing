package com.example.DistributedKafkaOrderProcessing.shipping;

import com.example.DistributedKafkaOrderProcessing.domain.enums.InventoryStatus;
import com.example.DistributedKafkaOrderProcessing.domain.enums.OrderStatus;
import com.example.DistributedKafkaOrderProcessing.domain.enums.ShippingStatus;
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
import java.util.UUID;

@Component
public class ShippingConsumer {

    private static final Logger log = LoggerFactory.getLogger(ShippingConsumer.class);
    private static  final String GROUP="shipping-service";

    private final ShippingService shippingService;
    private final ShippingEventProducer eventProducer;
    private final OrderService orderService;
    private final IdempotencyService idempotencyService;

    public ShippingConsumer(ShippingService shippingService, ShippingEventProducer eventProducer, OrderService orderService, IdempotencyService idempotencyService) {
        this.shippingService = shippingService;
        this.eventProducer = eventProducer;
        this.orderService = orderService;
        this.idempotencyService = idempotencyService;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.inventory-result}",
            groupId = GROUP,
            concurrency = "3"
    )
    public void handleInventoryResult(
            @Payload Events.InventoryResultEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack
            ){

        log.info("[shipping-service] InventoryResultEvent: orderId={} status={} partition={}",
                event.orderId(),event.status(),partition);

//        Only proceed on successful inventory reservation
        if(event.status() != InventoryStatus.RESERVED){
            log.info("[shipping-service] Skipping- inventory not reserved: orderId={}",event.orderId());
            ack.acknowledge();
            return;
        }

        // ── Idempotency ───────────────────────────────────────────────────────
        if (idempotencyService.alreadyProcessed(event.eventId(), GROUP)) {
            ack.acknowledge();
            return;
        }

        // ── Update order status ───────────────────────────────────────────────
        orderService.updateStatus(event.orderId(), OrderStatus.SHIPPING_BOOKING);

        var order = orderService.getOrder(event.orderId());
        var totalAmountWrapper = new ShippingService.BigDecimalWrapper(order.getTotalAmount());

        Events.ShippingResultEvent result= shippingService.bookShipment(
                event,
                order.getShippingAddress(),
                totalAmountWrapper
        );

//        Handle result
        if(result.status()== ShippingStatus.BOOKED){

//            SUCCESS path
            orderService.updateStatus(event.orderId(), OrderStatus.CONFIRMED);
            eventProducer.publishShippingResult(result);

            log.info("[shipping-service] Shipment booked: orderId={} carrier={} tracking={}",
                    event.orderId(),result.carrier(),result.trackingNumber());
        } else{

//        FAILURE PATH
        orderService.updateStatus(event.orderId(), OrderStatus.SHIPPING_FAILED);
        eventProducer.publishShippingResult(result);

        Events.CompensationEvent compensation = new Events.CompensationEvent(
                UUID.randomUUID().toString(),
                event.orderId(),
                event.customerId(),
                event.customerEmail(),
                null,
                event.transactionId(),          // ← for PaymentConsumer to refund
                "SHIPPING",
                result.failureReason(),
                true,                           // ← refundRequired: payment was charged
                true,                           // ← inventoryReleaseRequired: stock was reserved
                event.reservedItems(),          // ← exactly what to release
                order.getTotalAmount(),         // ← exact amount to refund
                order.getTotalAmount(),
                Instant.now()
        );
        eventProducer.publishCompensation(compensation);

        log.warn("[shipping-service] Booking failed, full compensation triggered: orderId={}",
                event.orderId());
    }

        ack.acknowledge();
}

}
