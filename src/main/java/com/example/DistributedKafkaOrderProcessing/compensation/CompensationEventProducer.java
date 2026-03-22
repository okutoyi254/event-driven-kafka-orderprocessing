package com.example.DistributedKafkaOrderProcessing.compensation;

import com.example.DistributedKafkaOrderProcessing.config.KafkaTopicProperties;
import com.example.DistributedKafkaOrderProcessing.domain.events.Events;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;


@Component
public class CompensationEventProducer {

    private static final Logger log= LoggerFactory.getLogger(CompensationEventProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaTopicProperties topics;

    public CompensationEventProducer(KafkaTemplate<String, Object> kafkaTemplate,
                                     KafkaTopicProperties topics) {
        this.kafkaTemplate = kafkaTemplate;
        this.topics = topics;
    }

    public void publishOrderFailed(Events.OrderFailedEvent event) {
        kafkaTemplate.send(topics.orderFailed(), event.orderId(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish OrderFailedEvent: orderId={}",
                                event.orderId(), ex);
                    } else {
                        log.info("OrderFailedEvent published: orderId={} stage={} → partition={}",
                                event.orderId(), event.failureStage(),
                                result.getRecordMetadata().partition());
                    }
                });
    }
}
