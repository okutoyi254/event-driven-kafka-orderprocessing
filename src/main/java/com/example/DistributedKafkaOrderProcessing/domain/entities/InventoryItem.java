package com.example.DistributedKafkaOrderProcessing.domain.entities;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
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

    public InventoryItem() {

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

    // Getters
    public String getProductId() { return productId; }
    public String getProductName() { return productName; }
    public String getCategory() { return category; }
    public int getQuantityAvailable() { return quantityAvailable; }
    public int getQuantityReserved() { return quantityReserved; }
    public String getWarehouseLocation() { return warehouseLocation; }

    // Setters
    public void setProductId(String productId) { this.productId = productId; }
    public void setProductName(String productName) { this.productName = productName; }
    public void setCategory(String category) { this.category = category; }
    public void setQuantityAvailable(int quantityAvailable) { this.quantityAvailable = quantityAvailable; }
    public void setQuantityReserved(int quantityReserved) { this.quantityReserved = quantityReserved; }
    public void setWarehouseLocation(String warehouseLocation) { this.warehouseLocation = warehouseLocation; }
}
