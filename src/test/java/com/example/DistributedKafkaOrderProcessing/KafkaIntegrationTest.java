package com.example.DistributedKafkaOrderProcessing;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@SpringBootTest
@EmbeddedKafka(
        partitions = 3,
        topics = {
                "order.placed",
                "payment.result",
                "inventory.result",
                "shipping.result",
                "order.confirmed",
                "order.failed",
                "order.compensation",
                "order.placed.DLT",
                "payment.result.DLT",
                "inventory.result.DLT",
                "shipping.result.DLT"
        }
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@ActiveProfiles("test")
public @interface KafkaIntegrationTest {
}