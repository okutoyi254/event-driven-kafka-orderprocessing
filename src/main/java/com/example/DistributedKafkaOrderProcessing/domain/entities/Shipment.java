package com.example.DistributedKafkaOrderProcessing.domain.entities;

import com.example.DistributedKafkaOrderProcessing.domain.enums.ShippingStatus;
import jakarta.persistence.*;

import java.time.Instant;
import java.time.LocalDate;

@Entity
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

    // Getters
    public Long getId() { return id; }
    public String getOrderId() { return orderId; }
    public String getCustomerId() { return customerId; }
    public ShippingStatus getStatus() { return status; }
    public String getTrackingNumber() { return trackingNumber; }
    public String getCarrier() { return carrier; }
    public LocalDate getEstimatedDelivery() { return estimatedDelivery; }
    public String getShippingAddress() { return shippingAddress; }
    public String getFailureReason() { return failureReason; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getCancelledAt() { return cancelledAt; }

    // Setters
    public void setId(Long id) { this.id = id; }
    public void setOrderId(String orderId) { this.orderId = orderId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    public void setStatus(ShippingStatus status) { this.status = status; }
    public void setTrackingNumber(String trackingNumber) { this.trackingNumber = trackingNumber; }
    public void setCarrier(String carrier) { this.carrier = carrier; }
    public void setEstimatedDelivery(LocalDate estimatedDelivery) { this.estimatedDelivery = estimatedDelivery; }
    public void setShippingAddress(String shippingAddress) { this.shippingAddress = shippingAddress; }
    public void setFailureReason(String failureReason) { this.failureReason = failureReason; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
    public void setCancelledAt(Instant cancelledAt) { this.cancelledAt = cancelledAt; }
}

