package com.example.DistributedKafkaOrderProcessing.domain.entities;

import com.example.DistributedKafkaOrderProcessing.domain.enums.OrderStatus;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
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
}
