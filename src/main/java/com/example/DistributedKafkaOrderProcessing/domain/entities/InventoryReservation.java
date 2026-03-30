package com.example.DistributedKafkaOrderProcessing.domain.entities;


import com.example.DistributedKafkaOrderProcessing.domain.enums.InventoryStatus;
import jakarta.persistence.*;

import java.time.Instant;

@Entity
@Table(name = "inventory_reservations")
public  class InventoryReservation{

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false) private String orderId;
    @Column(nullable = false) private String productId;
    @Column(nullable = false) private String productName;
    @Column(nullable = false) private int quantityReserved;
    @Column(nullable = false) private String warehouseLocation;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private InventoryStatus status;

    @Column(nullable = false) private Instant reservedAt;
    @Column private Instant releasedAt;

    protected InventoryReservation() {}

    public InventoryReservation(String orderId, String productId, String productName,
                                int quantityReserved, String warehouseLocation) {
        this.orderId = orderId;
        this.productId = productId;
        this.productName = productName;
        this.quantityReserved = quantityReserved;
        this.warehouseLocation = warehouseLocation;
        this.status = InventoryStatus.RESERVED;
        this.reservedAt = Instant.now();
    }

    public void release() {
        this.status = InventoryStatus.RELEASED;
        this.releasedAt = Instant.now();
    }

    // Getters
    public Long getId() { return id; }
    public String getOrderId() { return orderId; }
    public String getProductId() { return productId; }
    public String getProductName() { return productName; }
    public int getQuantityReserved() { return quantityReserved; }
    public String getWarehouseLocation() { return warehouseLocation; }
    public InventoryStatus getStatus() { return status; }
    public Instant getReservedAt() { return reservedAt; }
    public Instant getReleasedAt() { return releasedAt; }

    // Setters
    public void setId(Long id) { this.id = id; }
    public void setOrderId(String orderId) { this.orderId = orderId; }
    public void setProductId(String productId) { this.productId = productId; }
    public void setProductName(String productName) { this.productName = productName; }
    public void setQuantityReserved(int quantityReserved) { this.quantityReserved = quantityReserved; }
    public void setWarehouseLocation(String warehouseLocation) { this.warehouseLocation = warehouseLocation; }
    public void setStatus(InventoryStatus status) { this.status = status; }
    public void setReservedAt(Instant reservedAt) { this.reservedAt = reservedAt; }
    public void setReleasedAt(Instant releasedAt) { this.releasedAt = releasedAt; }
}

