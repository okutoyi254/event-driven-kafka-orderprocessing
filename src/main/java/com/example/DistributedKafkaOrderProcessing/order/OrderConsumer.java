package com.example.DistributedKafkaOrderProcessing.order;

import com.example.DistributedKafkaOrderProcessing.config.KafkaTopicProperties;
import com.example.DistributedKafkaOrderProcessing.domain.Entities;
import com.example.DistributedKafkaOrderProcessing.domain.enums.OrderStatus;
import com.example.DistributedKafkaOrderProcessing.domain.enums.ShippingStatus;
import com.example.DistributedKafkaOrderProcessing.domain.events.Events;
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
public class OrderConsumer {

    private static final Logger log= LoggerFactory.getLogger(OrderConsumer.class);
    private static final String GROUP= "order-service";

    private final OrderService orderService;
    private final OrderEventProducer eventProducer;
    private final IdempotencyService idempotencyService;
    private final KafkaTopicProperties topics;

    public OrderConsumer(OrderService orderService,
                         OrderEventProducer eventProducer,
                         IdempotencyService idempotencyService,
                         KafkaTopicProperties topics) {
        this.orderService = orderService;
        this.eventProducer = eventProducer;
        this.idempotencyService = idempotencyService;
        this.topics = topics;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.shipping-result}",
            groupId = GROUP,
            concurrency = "3"
    )
    public void handleShippingResult(
            @Payload Events.ShippingResultEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET)long offset,
            Acknowledgment ack
            ){

        log.info("[order-service] ShippingResultEvent: orderId={} status={} partition={}",
                event.orderId(),event.status(),partition);

        if(idempotencyService.alreadyProcessed(event.eventId(),GROUP)){
            ack.acknowledge();
            return;
        }

        if(event.status()== ShippingStatus.BOOKED){
            orderService.confirmOrder(event.orderId());
            orderService.updateStatus(event.orderId(), OrderStatus.CONFIRMED);

            Events.OrderConfirmedEvent confirmed= new Events.OrderConfirmedEvent(
                    UUID.randomUUID().toString(),
                    event.orderId(),
                    event.customerId(),
                    event.customerEmail(),
                    null,
                    event.totalAmount(),
                    event.transactionId(),
                    event.trackingNumber(),
                    event.carrier(),
                    event.estimatedDelivery(),
                    event.shippingAddress(),
                    event.reservedItems().stream()
                            .map(r->new Events.OrderItem(
                                    r.productId(),r.productName(),null,
                                    r.quantityReserved(),null,null
                            )).toList(),
                    Instant.now()
            );

            eventProducer.publishOrderConfirmed(confirmed);
            log.info("[order-service] Order CONFIRMED: orderId={} tracking={}",
                    event.orderId(),event.trackingNumber());
        }
        else {
            log.info("[order-service] Shipping unavailable for orderId={} - compensation in progress",
                    event.orderId());
        }
        ack.acknowledge();
    }
}
