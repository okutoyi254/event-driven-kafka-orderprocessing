package com.example.DistributedKafkaOrderProcessing.order;

import com.example.DistributedKafkaOrderProcessing.domain.Entities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class IdempotencyService {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyService.class);

    private final ProcessedEventRepository repository;

    public IdempotencyService(ProcessedEventRepository repository) {
        this.repository = repository;
    }
    @Transactional
    public boolean alreadyProcessed(String eventId, String consumerGroup) {
        if (repository.existsByEventIdAndConsumerGroup(eventId, consumerGroup)) {
            log.warn("⚠️  Duplicate event skipped: eventId={} group={}", eventId, consumerGroup);
            return true;
        }
        repository.save(new Entities.ProcessedEvent(eventId, consumerGroup));
        return false;
    }
}

@Repository
interface ProcessedEventRepository extends JpaRepository<Entities.ProcessedEvent, Long> {
    boolean existsByEventIdAndConsumerGroup(String eventId, String consumerGroup);
}

