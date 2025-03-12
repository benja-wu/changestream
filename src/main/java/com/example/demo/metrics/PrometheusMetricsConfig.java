package com.example.demo.metrics;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;

public class PrometheusMetricsConfig {
    private static final ConcurrentMap<String, PrometheusMetricsConfig> instances = new ConcurrentHashMap<>();
    private final Counter totalEventsHandled;
    private final Counter totalEventsHandledSuccessfully;
    private final Gauge eventLagPerThread;
    private final Gauge tpsPerThread;
    private final Histogram eventProcessDuration;
    private final Histogram p99ProcessingTime;

    /**  Private constructor: Prevents Spring from injecting */
    private PrometheusMetricsConfig(String collectionName) {
        this.totalEventsHandled = Counter.build()
                .name(collectionName + "_events_handled_total")
                .help("Total events handled for " + collectionName)
                .register();

        this.totalEventsHandledSuccessfully = Counter.build()
                .name(collectionName + "_events_success_total")
                .help("Total events successfully handled for " + collectionName)
                .register();

        this.eventLagPerThread = Gauge.build()
                .name(collectionName + "_event_lag_per_thread")
                .help("Event lag per thread for " + collectionName)
                .labelNames("thread")
                .register();

        this.tpsPerThread = Gauge.build()
                .name(collectionName + "_tps_per_thread")
                .help("TPS per thread for " + collectionName)
                .labelNames("thread")
                .register();

        this.eventProcessDuration = Histogram.build()
                .name(collectionName + "_event_process_duration_seconds")
                .help("Event processing duration in seconds for " + collectionName)
                .register();

        this.p99ProcessingTime = Histogram.build()
                .name(collectionName + "_p99_processing_time_millis")
                .help("P99 processing time in milliseconds for " + collectionName)
                .register();
    }

    /**  Static Factory Method: Ensures Singleton per Collection */
    public static PrometheusMetricsConfig getInstance(String collectionName) {
        return instances.computeIfAbsent(collectionName, PrometheusMetricsConfig::new);
    }

    public void incrementTotalEventsHandled() {
        totalEventsHandled.inc();
    }

    public void incrementTotalEventsHandledSuccessfully() {
        totalEventsHandledSuccessfully.inc();
    }

    public Gauge getEventLagPerThread() {
        return eventLagPerThread;
    }

    public Gauge getTpsPerThread() {
        return tpsPerThread;
    }

    public Histogram getEventProcessDuration() {
        return eventProcessDuration;
    }

    public Histogram getP99ProcessingTime() {
        return p99ProcessingTime;
    }
}
