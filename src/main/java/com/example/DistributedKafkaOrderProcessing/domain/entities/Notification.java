package com.example.DistributedKafkaOrderProcessing.domain.entities;

import com.example.DistributedKafkaOrderProcessing.domain.enums.NotificationStatus;
import com.example.DistributedKafkaOrderProcessing.domain.enums.NotificationType;
import jakarta.persistence.*;

import java.time.Instant;

@Entity
@Table(name = "notifications")
public  class Notification{

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
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

    // Getters
    public Long getId() { return id; }
    public String getOrderId() { return orderId; }
    public String getCustomerId() { return customerId; }
    public String getCustomerEmail() { return customerEmail; }
    public String getCustomerPhone() { return customerPhone; }
    public NotificationType getType() { return type; }
    public NotificationStatus getStatus() { return status; }
    public String getSubject() { return subject; }
    public String getMessage() { return message; }
    public Instant getDeadline() { return deadline; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getSentAt() { return sentAt; }
    public int getRetryCount() { return retryCount; }

    // Setters
    public void setId(Long id) { this.id = id; }
    public void setOrderId(String orderId) { this.orderId = orderId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    public void setCustomerEmail(String customerEmail) { this.customerEmail = customerEmail; }
    public void setCustomerPhone(String customerPhone) { this.customerPhone = customerPhone; }
    public void setType(NotificationType type) { this.type = type; }
    public void setStatus(NotificationStatus status) { this.status = status; }
    public void setSubject(String subject) { this.subject = subject; }
    public void setMessage(String message) { this.message = message; }
    public void setDeadline(Instant deadline) { this.deadline = deadline; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
    public void setSentAt(Instant sentAt) { this.sentAt = sentAt; }
    public void setRetryCount(int retryCount) { this.retryCount = retryCount; }
}

