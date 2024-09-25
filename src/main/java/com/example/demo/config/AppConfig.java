package com.example.demo.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.annotation.EnableRetry;

@Configuration
@EnableRetry
public class AppConfig {

        @Bean
        public double tpsAlphaValue() {
                return 0.1; // Set alpha value for TPS calculations
        }
}
