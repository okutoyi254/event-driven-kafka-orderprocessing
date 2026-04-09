package com.example.DistributedKafkaOrderProcessing.payment;

import com.example.DistributedKafkaOrderProcessing.domain.entities.Payment;
import com.example.DistributedKafkaOrderProcessing.domain.enums.PaymentStatus;
import com.example.DistributedKafkaOrderProcessing.domain.events.Events;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Random;
import java.util.UUID;


@Service
public class PaymentService {

    private final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentRepository paymentRepository;
    private final Random random=new Random();

    public PaymentService(PaymentRepository paymentRepository){
        this.paymentRepository= paymentRepository;
    }

    @Transactional
    public Events.PaymentResultEvent processPayment(Events.OrderPlacedEvent event) {

        // ── Simulate gateway call ─────────────────────────────────────────────
        int outcome = random.nextInt(10);

        if (outcome == 0) {
            throw new PaymentGatewayTimeoutException(
                    "Payment gateway timeout for order: " + event.orderId());
        }

        // Determine result
        PaymentStatus status;
        String transactionId = null;
        String failureReason = null;

        if (outcome <= 7) {
            // 70% success
            status = PaymentStatus.SUCCESS;
            transactionId = "TXN-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
            log.info("💳 Payment SUCCESS: orderId={} txn={} amount={}",
                    event.orderId(), transactionId, event.totalAmount());

        } else {
            // 20% insufficient funds
            status = PaymentStatus.INSUFFICIENT_FUNDS;
            failureReason = "Insufficient funds — card ending in "
                    + (1000 + random.nextInt(9000)) + " declined";
            log.warn("💳 Payment FAILED: orderId={} reason={}", event.orderId(), failureReason);
        }

        // ── Save payment record ───────────────────────────────────────────────
        Payment payment = new Payment(
                event.orderId(),
                event.customerId(),
                transactionId,
                status,
                event.totalAmount(),
                failureReason
        );
        paymentRepository.save(payment);

        // ── Build result event ────────────────────────────────────────────────
        return new Events.PaymentResultEvent(
                UUID.randomUUID().toString(),
                event.orderId(),
                event.customerId(),
                event.customerEmail(),
                status,
                transactionId,
                event.totalAmount(),
                failureReason,
                Instant.now()
        );
    }

//    Refund a payment - called by CompensationConsumer on saga rollback
    @Transactional
    public boolean refundPayment(String orderId, String transactionId){

        return paymentRepository.findByOrderId(orderId)
                .map(payment -> {
                    if(payment.getStatus() == PaymentStatus.SUCCESS){

//                        Simulate refund API call
                        simulateWork(50,150);
                        payment.refund();
                        paymentRepository.save(payment);
                        log.info("Refund processed: orderId={} txn={} amount={}",orderId,transactionId,payment.getAmount());
                        return true;
                    }
                    else{
                        log.info("No refund needed: orderId={} status={}",orderId,payment.getStatus());
                        return false;
                    }
                }).orElse(false);
    }

    private void simulateWork(int minMs,int maxMs){
        try{
            Thread.sleep(minMs+random.nextInt(maxMs-minMs));
        } catch (InterruptedException ex){
            Thread.currentThread().interrupt();
        }
    }

    public static class PaymentGatewayTimeoutException extends RuntimeException{

        public PaymentGatewayTimeoutException(String message){
            super(message);
        }
    }
}
