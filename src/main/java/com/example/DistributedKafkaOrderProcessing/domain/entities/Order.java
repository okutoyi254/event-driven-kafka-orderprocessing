package com.example.DistributedKafkaOrderProcessing.domain.entities;

import com.example.DistributedKafkaOrderProcessing.domain.enums.OrderStatus;
import jakarta.persistence.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

@Entity
@Table(name = "orders")
public  class Order{

    @Id
    private String id;

    @Column(nullable = false) private String customerId;
    @Column(nullable = false) private String customerEmail;
    @Column(nullable = false) private String customerPhone;

    @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.EAGER)
    @JoinColumn(name = "order_id")
    private List<OrderItem> items;

    @Column(nullable = false)
    private String shippingAddress;

    @Column(nullable = false, precision = 10, scale = 2)
    private BigDecimal totalAmount;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OrderStatus status;

    @Column(nullable = false) private Instant placedAt;
    @Column private Instant confirmedAt;
    @Column private Instant updatedAt;
    @Column private String failureReason;

    public Order(String id, String customerId, String customerEmail, String customerPhone, List<OrderItem> items, BigDecimal totalAmount, String shippingAddress) {
        this.id = id;
        this.customerId = customerId;
        this.customerEmail = customerEmail;
        this.customerPhone = customerPhone;
        this.items = items;
        this.totalAmount = totalAmount;
        this.shippingAddress = shippingAddress;

    }

    public Order() {

    }

    public void updateStatus(OrderStatus status) {
        this.status = status;
        this.updatedAt = Instant.now();
    }

    public void confirm() {
        this.status = OrderStatus.CONFIRMED;
        this.confirmedAt = Instant.now();
        this.updatedAt = Instant.now();
    }

    public void fail(String reason) {
        this.status = OrderStatus.PAYMENT_FAILED;
        this.failureReason = reason;
        this.updatedAt = Instant.now();
    }

    // Getters
    public String getId() { return id; }
    public String getCustomerId() { return customerId; }
    public String getCustomerEmail() { return customerEmail; }
    public String getCustomerPhone() { return customerPhone; }
    public List<OrderItem> getItems() { return items; }
    public String getShippingAddress() { return shippingAddress; }
    public BigDecimal getTotalAmount() { return totalAmount; }
    public OrderStatus getStatus() { return status; }
    public Instant getPlacedAt() { return placedAt; }
    public Instant getConfirmedAt() { return confirmedAt; }
    public Instant getUpdatedAt() { return updatedAt; }
    public String getFailureReason() { return failureReason; }

    // Setters
    public void setId(String id) { this.id = id; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    public void setCustomerEmail(String customerEmail) { this.customerEmail = customerEmail; }
    public void setCustomerPhone(String customerPhone) { this.customerPhone = customerPhone; }
    public void setItems(List<OrderItem> items) { this.items = items; }
    public void setShippingAddress(String shippingAddress) { this.shippingAddress = shippingAddress; }
    public void setTotalAmount(BigDecimal totalAmount) { this.totalAmount = totalAmount; }
    public void setStatus(OrderStatus status) { this.status = status; }
    public void setPlacedAt(Instant placedAt) { this.placedAt = placedAt; }
    public void setConfirmedAt(Instant confirmedAt) { this.confirmedAt = confirmedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }
    public void setFailureReason(String failureReason) { this.failureReason = failureReason; }
}
