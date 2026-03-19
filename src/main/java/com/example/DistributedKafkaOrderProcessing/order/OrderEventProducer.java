package com.example.DistributedKafkaOrderProcessing.order;

import com.example.DistributedKafkaOrderProcessing.config.KafkaTopicProperties;
import com.example.DistributedKafkaOrderProcessing.domain.events.Events;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class OrderEventProducer {

    private static final Logger log= LoggerFactory.getLogger(OrderEventProducer.class);

    private final KafkaTemplate<String,Object> kafkaTemplate;
    private final KafkaTopicProperties topics;

    public OrderEventProducer(KafkaTemplate<String, Object> kafkaTemplate,
                              KafkaTopicProperties topics) {
        this.kafkaTemplate = kafkaTemplate;
        this.topics = topics;
    }

//    ----Publish OrderPlacedEvent-------------------
public void publishOrderPlaced(Events.OrderPlacedEvent event) {
    kafkaTemplate.send(topics.orderPlaced(), event.orderId(), event)
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("❌ Failed to publish OrderPlacedEvent: orderId={} error={}",
                            event.orderId(), ex.getMessage(), ex);
                    // Production: persist to outbox table, retry async
                } else {
                    log.info("OrderPlacedEvent published: orderId={} → partition={} offset={}",
                            event.orderId(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                }
            });
}

//-----Publish OrderConfirmedEvent----------------------
    public void publishOrderConfirmed(Events.OrderConfirmedEvent event){

        kafkaTemplate.send(topics.orderConfirmed(), event.orderId(),event)
                .whenComplete((result,ex)->{
                    if(ex !=null){
                        log.error("Failed to publish OrderConfirmedEvent: orderId={}",event.orderId(),ex);
                    }
                    else{
                        log.info("OrderConfirmedEvent published: orderId={}",event.orderId());
                    }
                });
    }

//    -----Publish OrderFailedEvent---------------------
public void publishOrderFailed(Events.OrderFailedEvent event) {
    kafkaTemplate.send(topics.orderFailed(), event.orderId(), event)
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("❌ Failed to publish OrderFailedEvent: orderId={}",
                            event.orderId(), ex);
                } else {
                    log.info("✅ OrderFailedEvent published: orderId={} stage={}",
                            event.orderId(), event.failureStage());
                }
            });
}
    }

