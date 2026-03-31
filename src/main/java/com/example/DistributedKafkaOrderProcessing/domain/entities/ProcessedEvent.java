package com.example.DistributedKafkaOrderProcessing.domain.entities;

import jakarta.persistence.*;

import java.time.Instant;

//    Idempotency
@Entity
@Table(name = "processed_events",
        uniqueConstraints = @UniqueConstraint(columnNames = {"eventId","consumerGroup"}))
public  class ProcessedEvent{

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
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

    // Getters
    public Long getId() { return id; }
    public String getEventId() { return eventId; }
    public String getConsumerGroup() { return consumerGroup; }
    public Instant getProcessedAt() { return processedAt; }

    // Setters
    public void setId(Long id) { this.id = id; }
    public void setEventId(String eventId) { this.eventId = eventId; }
    public void setConsumerGroup(String consumerGroup) { this.consumerGroup = consumerGroup; }
    public void setProcessedAt(Instant processedAt) { this.processedAt = processedAt; }
}
