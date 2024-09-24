package com.example.demo.metrics;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.Summary;

@Configuration
public class PrometheusMetricsConfig {

        @Bean
        public Counter totalEventsHandled() {
                return Counter.build().name("total_events_handled")
                                .help("Total number of events handled across all threads.").register();
        }

        @Bean
        public Counter totalEventsHandledSuccessfully() {
                return Counter.build().name("total_events_handled_successfully")
                                .help("Total number of successful events handled across all threads.").register();
        }

        @Bean
        public Gauge eventLagPerThread() {
                return Gauge.build().name("event_lag_per_thread").help("Real-time event lag per thread.")
                                .labelNames("thread_name").register();
        }

        @Bean
        public Histogram eventProcessDuration() {
                return Histogram.build().name("event_process_duration_seconds")
                                .help("Histogram for tracking event processing duration.")
                                .buckets(0.0, 0.05, 0.1, 0.2, 0.5, 0.7, 1, 2).register();
        }

        @Bean
        public Gauge tpsPerThread() {
                return Gauge.build().name("tps_per_thread")
                                .help("TPS as exponentially-weighted moving average in last 15 minutes per thread.")
                                .labelNames("thread_name").register();
        }

        @Bean
        public Summary p99ProcessingTime() {
                return Summary.build().name("p99_processing_time_milliseconds")
                                .help("Processing time for 99% of the requests in milliseconds.").quantile(0.99, 0.01) // 99th
                                                                                                                       // percentile
                                                                                                                       // with
                                                                                                                       // tolerated
                                                                                                                       // error
                                                                                                                       // margin
                                .register();
        }
}
