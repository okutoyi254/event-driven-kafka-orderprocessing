package com.example.DistributedKafkaOrderProcessing.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

@Configuration
@EnableKafka
@EnableConfigurationProperties(KafkaTopicProperties.class)
public class KafkaConfig {

    private final KafkaTopicProperties topics;


    public KafkaConfig(KafkaTopicProperties topics) {
        this.topics = topics;
    }

//    Topics Definitions
@Bean public NewTopic orderPlacedTopic() {
    return TopicBuilder.name(topics.orderPlaced()).partitions(3).replicas(1).build();
}
    @Bean public NewTopic paymentResultTopic() {
        return TopicBuilder.name(topics.paymentResult()).partitions(3).replicas(1).build();
    }
    @Bean public NewTopic inventoryResultTopic() {
        return TopicBuilder.name(topics.inventoryResult()).partitions(3).replicas(1).build();
    }
    @Bean public NewTopic shippingResultTopic() {
        return TopicBuilder.name(topics.shippingResult()).partitions(3).replicas(1).build();
    }
    @Bean public NewTopic orderConfirmedTopic() {
        return TopicBuilder.name(topics.orderConfirmed()).partitions(3).replicas(1).build();
    }
    @Bean public NewTopic orderFailedTopic() {
        return TopicBuilder.name(topics.orderFailed()).partitions(3).replicas(1).build();
    }
    @Bean public NewTopic compensationTopic() {
        return TopicBuilder.name(topics.compensation()).partitions(3).replicas(1).build();
    }

    // ── Dead Letter Topics ────────────────────────────────────────────────────
    @Bean public NewTopic orderPlacedDlt() {
        return TopicBuilder.name(topics.orderPlacedDlt()).partitions(3).replicas(1).build();
    }
    @Bean public NewTopic paymentResultDlt() {
        return TopicBuilder.name(topics.paymentResultDlt()).partitions(3).replicas(1).build();
    }
    @Bean public NewTopic inventoryResultDlt() {
        return TopicBuilder.name(topics.inventoryResultDlt()).partitions(3).replicas(1).build();
    }
    @Bean public NewTopic shippingResultDlt() {
        return TopicBuilder.name(topics.shippingResultDlt()).partitions(3).replicas(1).build();
    }

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<Object,Object>kafkaTemplate){

        var backOff= new ExponentialBackOffWithMaxRetries(3);
        backOff.setInitialInterval(1_000L);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(10_000);

        var recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);
        var handler=new DefaultErrorHandler(recoverer,backOff);

        handler.addNotRetryableExceptions(
                IllegalArgumentException.class,
                NullPointerException.class
        );

        return handler;
    }

//    Listener factory
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory
            (
                    ConsumerFactory<String,Object> consumerFactory,
                    DefaultErrorHandler errorHandler
            ){

        var factory = new ConcurrentKafkaListenerContainerFactory<String,Object>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }
}
