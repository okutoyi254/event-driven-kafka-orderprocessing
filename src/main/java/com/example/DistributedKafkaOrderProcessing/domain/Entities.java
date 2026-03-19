package com.example.DistributedKafkaOrderProcessing.domain;

import com.example.DistributedKafkaOrderProcessing.domain.enums.*;
import jakarta.persistence.*;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

public final class Entities {

    private Entities(){}

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Entity
    @Table(name = "orders")
    public static class Order{

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

    @Entity
    @Table(name = "order_items")
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class OrderItem {

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


    @AllArgsConstructor
    @NoArgsConstructor
    @Entity
    @Data
    @Table(name = "payments")
    public static class  Payment{

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

    }

    @Entity
    @NoArgsConstructor
    @Data
    @Table(name = "inventory_items")
    private static class InventoryItem{

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

    @Entity
    @Data
    @Table(name = "inventory_reservations")
    public static class InventoryReservation{

        @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
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

    @Entity
    @Data
    @Table(name = "shipments")
    public static class Shipment{

        @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
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

    @Entity
    @Table(name = "notifications")
    public static class Notification{

        @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
        private Long id;

        @Column(nullable = false) private String orderId;
        @Column(nullable = false) private String customerId;
        @Column(nullable = false) private String customerEmail;
        @Column private String customerPhone;

        @Enumerated(EnumType.STRING)
        @Column(nullable = false)
        private NotificationType type;

        @Enumerated(EnumType.STRING)
        @Column(nullable = false)
        private NotificationStatus status;

        @Column(nullable = false, length = 500) private String subject;
        @Column(nullable = false, length = 2000) private String message;

        // SLA: must be sent within 5 minutes of order placed
        @Column(nullable = false) private Instant deadline;
        @Column(nullable = false) private Instant createdAt;
        @Column private Instant sentAt;
        @Column private int retryCount;

        protected Notification() {}

        public Notification(String orderId, String customerId, String customerEmail,
                            String customerPhone, NotificationType type,
                            String subject, String message, Instant deadline) {
            this.orderId = orderId;
            this.customerId = customerId;
            this.customerEmail = customerEmail;
            this.customerPhone = customerPhone;
            this.type = type;
            this.subject = subject;
            this.message = message;
            this.deadline = deadline;
            this.status = NotificationStatus.PENDING;
            this.createdAt = Instant.now();
            this.retryCount = 0;
        }

        public void markSent(){

            this.status = NotificationStatus.SENT;
            this.sentAt = Instant.now();
        }

        public void markFailed() {
            this.status = NotificationStatus.FAILED;
            this.retryCount++;
        }
    }

//    Idempotency
    @Getter
    @Setter
    @Entity
    @Table(name = "processed_events",
    uniqueConstraints = @UniqueConstraint(columnNames = {"eventId","consumerGroup"}))
            public static class ProcessedEvent{

        @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
        private Long id;

        @Column(nullable = false) private String eventId;
        @Column(nullable = false) private String consumerGroup;
        @Column(nullable = false) private Instant processedAt;

        protected ProcessedEvent() {}

        public ProcessedEvent(String eventId, String consumerGroup) {
            this.eventId = eventId;
            this.consumerGroup = consumerGroup;
            this.processedAt = Instant.now();
        }

    }
}
