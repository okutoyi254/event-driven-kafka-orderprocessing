package com.example.DistributedKafkaOrderProcessing.domain.entities;

import com.example.DistributedKafkaOrderProcessing.domain.enums.PaymentStatus;
import jakarta.persistence.*;

import java.math.BigDecimal;
import java.time.Instant;

@Entity
@Table(name = "payments")
public  class  Payment{

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false,unique = true) private String orderId;
    @Column(nullable = false) private String customerId;
    @Column private String transactionId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private PaymentStatus status;

    @Column(nullable = false, precision = 10, scale = 2) private BigDecimal amount;
    @Column private String failureReason;
    @Column(nullable = false) private Instant processedAt;
    @Column private Instant refundedAt;

    public Payment(String orderId, String customerId, String transactionId, PaymentStatus status, BigDecimal amount, String failureReason) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.transactionId = transactionId;
        this.status = status;
        this.amount = amount;
        this.failureReason = failureReason;
    }

    public Payment() {

    }

    public void refund() {
        this.status = PaymentStatus.REFUNDED;
        this.refundedAt = Instant.now();
    }

    // Getters
    public Long getId() { return id; }
    public String getOrderId() { return orderId; }
    public String getCustomerId() { return customerId; }
    public String getTransactionId() { return transactionId; }
    public PaymentStatus getStatus() { return status; }
    public BigDecimal getAmount() { return amount; }
    public String getFailureReason() { return failureReason; }
    public Instant getProcessedAt() { return processedAt; }
    public Instant getRefundedAt() { return refundedAt; }

    // Setters
    public void setId(Long id) { this.id = id; }
    public void setOrderId(String orderId) { this.orderId = orderId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }
    public void setStatus(PaymentStatus status) { this.status = status; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
    public void setFailureReason(String failureReason) { this.failureReason = failureReason; }
    public void setProcessedAt(Instant processedAt) { this.processedAt = processedAt; }
    public void setRefundedAt(Instant refundedAt) { this.refundedAt = refundedAt; }
}
