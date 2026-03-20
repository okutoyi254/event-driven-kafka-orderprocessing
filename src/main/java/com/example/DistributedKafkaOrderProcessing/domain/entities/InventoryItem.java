package com.example.DistributedKafkaOrderProcessing.domain.entities;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@NoArgsConstructor
@Data
@Table(name = "inventory_items")
public  class InventoryItem{

    @Id
    private String productId;

    @Column(nullable = false) private String productName;
    @Column(nullable = false) private String category;
    @Column(nullable = false) private int quantityAvailable;
    @Column(nullable = false) private int quantityReserved;
    @Column(nullable = false) private String warehouseLocation;

    public InventoryItem(String productId, String productName, String category,
                         int quantityAvailable,String warehouseLocation){
        this.productId=productId;
        this.productName=productName;
        this.category=category;
        this.quantityAvailable=quantityAvailable;
        this.quantityReserved= 0;
        this.warehouseLocation=warehouseLocation;
    }
    public boolean canReserve(int quantity){
        return (quantityAvailable- quantityReserved)>= quantity;
    }

    public void reserve(int quantity){
        this.quantityReserved+= quantity;
    }
    public void release(int quantity){
        this.quantityReserved = Math.max(0,this.quantityReserved- quantity);
    }
}

