package com.example.DistributedKafkaOrderProcessing.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.Contact;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Distributed Kafka Order Processing API")
                        .version("0.0.1")
                        .description("API for the Distributed Kafka Order Processing demo application")
                        .contact(new Contact().name("Example Team").email("dev@example.com"))
                );
    }
}

