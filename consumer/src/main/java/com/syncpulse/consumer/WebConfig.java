package com.syncpulse.consumer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebConfig {

    @Bean
    public WebMvcConfigurer corsConfigurer(
            @Value("${syncpulse.cors.allowed-origins}") String[] origins) {
        return new WebMvcConfigurer() {
            @Override
            public void addCorsMappings(CorsRegistry registry) {
                registry.addMapping("/consumer/**")
                        .allowedOrigins(origins)
                        .allowedMethods("GET", "OPTIONS")
                        .maxAge(3600);
            }
        };
    }
}
