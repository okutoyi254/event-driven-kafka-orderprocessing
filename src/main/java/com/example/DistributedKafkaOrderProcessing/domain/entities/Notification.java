package com.example.DistributedKafkaOrderProcessing.domain.entities;

import com.example.DistributedKafkaOrderProcessing.domain.enums.NotificationStatus;
import com.example.DistributedKafkaOrderProcessing.domain.enums.NotificationType;
import jakarta.persistence.*;
import lombok.Data;

import java.time.Instant;

@Entity
@Data
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
}

