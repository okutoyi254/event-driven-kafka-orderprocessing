package com.example.DistributedKafkaOrderProcessing.domain.entities;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Entity
@Table(name = "order_items")
@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderItem {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String productId;

    public OrderItem( String productId, String productName, String productCategory, int quantity, BigDecimal unitPrice) {
        this.productId = productId;
        this.productName = productName;
        this.productCategory = productCategory;
        this.quantity = quantity;
        this.unitPrice = unitPrice;
    }

    @Column(nullable = false)
    private String productName;
    @Column(nullable = false)
    private String productCategory;
    @Column(nullable = false)
    private int quantity;
    @Column(nullable = false, precision = 10, scale = 2)
    private BigDecimal unitPrice;
    @Column(nullable = false, precision = 10, scale = 2)
    private BigDecimal subtotal;

    public OrderItem( String productId, String productName, String productCategory, int quantity, BigDecimal unitPrice, BigDecimal subtotal) {
        this.productId = productId;
        this.productName = productName;
        this.productCategory = productCategory;
        this.quantity = quantity;
        this.unitPrice = unitPrice;
        this.subtotal = subtotal;
    }
}


