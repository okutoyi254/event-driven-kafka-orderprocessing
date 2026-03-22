package com.example.DistributedKafkaOrderProcessing.shipping;

import com.example.DistributedKafkaOrderProcessing.config.KafkaTopicProperties;
import com.example.DistributedKafkaOrderProcessing.domain.events.Events;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class ShippingEventProducer {

    private static final Logger log = LoggerFactory.getLogger(ShippingEventProducer.class);

    private final KafkaTemplate<String,Object> kafkaTemplate;
    private final KafkaTopicProperties topics;

    public ShippingEventProducer(KafkaTemplate<String, Object> kafkaTemplate, KafkaTopicProperties kafkaTopicProperties) {
        this.kafkaTemplate = kafkaTemplate;
        this.topics = kafkaTopicProperties;
    }

    public void publishShippingResult(Events.ShippingResultEvent event){

        kafkaTemplate.send(topics.shippingResult(),event.orderId(),event)
                .whenComplete((result,ex)->{
                    if(ex !=null){
                        log.error("Failed to publish ShippingResultEvent: orderId={}",
                                event.orderId(),ex);
                    } else{
                        log.error("ShippingResultEvent published: orderId={} status={} ->partition={}",
                                event.orderId(),event.status(),result.getRecordMetadata().partition());
                    }
                });
    }

    public void publishCompensation(Events.CompensationEvent event){

        kafkaTemplate.send(topics.compensation(),event.orderId(),event)
                .whenComplete((result,ex)->
                {
                    if(ex !=null){
                    log.error("Failed to publish CompensationEvent: orderId={}",
                            event.orderId(), ex);
                } else {
            log.info("CompensationEvent published: orderId={} stage={} " +
                            "refund={} releaseInventory={}",
                    event.orderId(), event.failureStage(),
                    event.refundRequired(), event.inventoryReleaseRequired());
        }
    });

    }
}
