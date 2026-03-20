package com.example.DistributedKafkaOrderProcessing.domain.entities;


import com.example.DistributedKafkaOrderProcessing.domain.enums.InventoryStatus;
import jakarta.persistence.*;
import lombok.Data;

import java.time.Instant;

@Entity
@Data
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

}

