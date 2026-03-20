package com.example.DistributedKafkaOrderProcessing.domain.entities;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.time.Instant;

//    Idempotency
@Getter
@Setter
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

}
