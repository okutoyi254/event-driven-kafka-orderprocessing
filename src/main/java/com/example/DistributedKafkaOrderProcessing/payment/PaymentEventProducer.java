package com.example.DistributedKafkaOrderProcessing.payment;

import com.example.DistributedKafkaOrderProcessing.config.KafkaTopicProperties;
import com.example.DistributedKafkaOrderProcessing.domain.events.Events;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class PaymentEventProducer {

    private final Logger log = LoggerFactory.getLogger(PaymentEventProducer.class);

    private final KafkaTemplate<String,Object> kafkaTemplate;
    private final KafkaTopicProperties topics;

    public PaymentEventProducer(KafkaTemplate<String, Object> kafkaTemplate,
                                KafkaTopicProperties topics) {
        this.kafkaTemplate = kafkaTemplate;
        this.topics = topics;
    }

    public void publishPaymentResult(Events.PaymentResultEvent event){

        kafkaTemplate.send(topics.paymentResult(),event.orderId(),event)
                .whenComplete((result,ex)->{
                    if(ex !=null){
                        log.error("Failed to publish PaymentResultEvent: orderId={}",
                                event.orderId(),ex);
                    }
                    else{
                        log.info("PaymentResultEvent published: orderId={} status={} ->partition={}",
                                event.orderId(),event.status()
                        ,result.getRecordMetadata().partition());
                    }
                });
    }

    public void publishCompensation(Events.CompensationEvent event){

        kafkaTemplate.send(topics.compensation(), event.orderId(),event)
                .whenComplete((result,ex)->{
                    if(ex!=null){
                        log.error("Failed to publish CompensationEvent: orderId={}",
                                event.orderId(),ex);
                    }
                    else{
                        log.info("CompensationEvent published: orderId={} stage={}",
                                event.orderId(),event.failureStage());
                    }
                });
    }
}
