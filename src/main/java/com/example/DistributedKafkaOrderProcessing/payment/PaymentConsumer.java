package com.example.DistributedKafkaOrderProcessing.payment;

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
public class PaymentConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentConsumer.class);

    private static final String GROUP= "payment-service";
    private final PaymentService paymentService;
    private final PaymentEventProducer eventProducer;
    private final OrderService orderService;
    private final IdempotencyService idempotencyService;

    public PaymentConsumer(PaymentService paymentService, PaymentEventProducer eventProducer, OrderService orderService, IdempotencyService idempotencyService) {
        this.paymentService = paymentService;
        this.eventProducer = eventProducer;
        this.orderService = orderService;
        this.idempotencyService = idempotencyService;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.order-placed}",
            groupId = GROUP,
            concurrency = "3"
    )
    public void handleOrderPlaced(
            @Payload Events.OrderPlacedEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack
            )
    {
        log.info("[payment-service] OrderPlacedEvent: orderId={} amount={} partition={}",
                event.orderId(),event.totalAmount(),partition);

//        Idempotency
        if(idempotencyService.alreadyProcessed(event.eventId(), GROUP)){
            ack.acknowledge();
            return;
    }

//        Mark order as payment processing
        orderService.updateStatus(event.orderId(), OrderStatus.PAYMENT_PROCESSING);

//        Process payment
        try {
            Events.PaymentResultEvent result = paymentService.processPayment(event);

//            Update order status
            if (result.status() == PaymentStatus.SUCCESS) {
                orderService.updateStatus(event.orderId(), OrderStatus.INVENTORY_CHECKING);
            } else {
                orderService.updateStatus(event.orderId(), OrderStatus.PAYMENT_FAILED);
            }

//        Publish payment result
            eventProducer.publishPaymentResult(result);

//            On failure, publish compensation event
            if(result.status() != PaymentStatus.SUCCESS){

                Events.CompensationEvent compensation = new Events.CompensationEvent(
                        UUID.randomUUID().toString(),
                        event.orderId(),
                        event.customerId(),
                        event.customerEmail(),
                        event.customerPhone(),
                        null,
                        "PAYMENT",
                        result.failureReason(),
                        false,
                        false,
                        List.of(),
                        event.totalAmount(),
                        event.totalAmount(),
                        Instant.now()
                );
                eventProducer.publishCompensation(compensation);
                log.warn("[payment-service] Payment failed, compensation triggerd: orderId={}",
                        event.orderId());
            }
        }
        finally {
            ack.acknowledge();
        }
    }

    @KafkaListener(
            topics = "${app.kafka.topics.compensation}",
            groupId = GROUP+"-compensation",
            concurrency = "3"
    )
    public void handleCompensation(
            @Payload Events.CompensationEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partitions,
            Acknowledgment ack
            ){

        log.info("[payment-service] CompensationEvet: orderId={} refundRequired={} stage={}",
                event.orderId(),event.refundRequired(),event.failureStage());

        String idempotencyKey= event.eventId()+"-payment-compensation";
        if(idempotencyService.alreadyProcessed(idempotencyKey,GROUP+"-compensation")){
            ack.acknowledge();
            return;
        }

        if(event.refundRequired() && event.transactionId() !=null) {
            boolean refunded = paymentService.refundPayment(event.orderId(), event.transactionId());

            if (refunded) {
                log.info("[payment-service] Refund completed: orderId={} txn={}",
                        event.orderId(), event.transactionId());
            } else {
                log.warn("[payment-service] Refund not applicable: orderId={}", event.orderId());
            }
        }
            else{
                log.info("[payment-service] No refund required for orderId={}",event.orderId());
            }
            ack.acknowledge();
        }
    }

