package com.example.DistributedKafkaOrderProcessing.domain.entities;

import com.example.DistributedKafkaOrderProcessing.domain.enums.PaymentStatus;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

@AllArgsConstructor
@NoArgsConstructor
@Entity
@Data
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

    public void refund() {
        this.status = PaymentStatus.REFUNDED;
        this.refundedAt = Instant.now();
    }
}

