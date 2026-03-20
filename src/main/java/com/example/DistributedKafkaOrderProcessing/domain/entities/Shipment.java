package com.example.DistributedKafkaOrderProcessing.domain.entities;

import com.example.DistributedKafkaOrderProcessing.domain.enums.ShippingStatus;
import jakarta.persistence.*;
import lombok.Data;

import java.time.Instant;
import java.time.LocalDate;

@Entity
@Data
@Table(name = "shipments")
public  class Shipment{

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true) private String orderId;
    @Column(nullable = false) private String customerId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ShippingStatus status;

    @Column private String trackingNumber;
    @Column private String carrier;
    @Column private LocalDate estimatedDelivery;
    @Column(nullable = false) private String shippingAddress;
    @Column private String failureReason;
    @Column(nullable = false) private Instant createdAt;
    @Column private Instant cancelledAt;

    protected Shipment() {}

    public Shipment(String orderId, String customerId, ShippingStatus status,
                    String trackingNumber, String carrier,
                    LocalDate estimatedDelivery, String shippingAddress,
                    String failureReason) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.status = status;
        this.trackingNumber = trackingNumber;
        this.carrier = carrier;
        this.estimatedDelivery = estimatedDelivery;
        this.shippingAddress = shippingAddress;
        this.failureReason = failureReason;
        this.createdAt = Instant.now();
    }

    public void cancel() {
        this.status = ShippingStatus.CANCELLED;
        this.cancelledAt = Instant.now();
    }
}

