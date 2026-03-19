package com.example.DistributedKafkaOrderProcessing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class DistributedKafkaOrderProcessingApplication {

	private static  final Logger log = LoggerFactory.getLogger(DistributedKafkaOrderProcessingApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(DistributedKafkaOrderProcessingApplication.class, args);
		log.info(
				"""
						            ╔══════════════════════════════════════════════════════════════════╗
						            ║        Hardware Store — Order Processing System                  ║
						            ╠══════════════════════════════════════════════════════════════════╣
						            ║  API:          http://localhost:8082/api/orders                  ║
						            ║  H2 Console:   http://localhost:8082/h2-console                  ║
						            ║  Kafka UI:     http://localhost:8080                             ║
						            ║  Health:       http://localhost:8082/actuator/health             ║
						            ╠══════════════════════════════════════════════════════════════════╣
						            ║  Quick test:                                                     ║
						            ║  curl -X POST http://localhost:8082/api/orders/batch?count=5     ║
						            ╚══════════════════════════════════════════════════════════════════╝
						""");
	}

}
