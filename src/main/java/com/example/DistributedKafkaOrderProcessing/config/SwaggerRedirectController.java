package com.example.DistributedKafkaOrderProcessing.config;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class SwaggerRedirectController {

    @GetMapping("/swagger")
    public String swaggerRedirect() {
        // Springdoc serves the UI at /swagger-ui.html or /swagger-ui/index.html
        return "redirect:/swagger-ui/index.html";
    }
}

