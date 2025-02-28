package com.example.demo.metrics;

import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

@Component
public class TpsCalculator {
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, LinkedList<Long>>> statsMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, Double>> tpsMap = new ConcurrentHashMap<>(); // Store the current TPS per collection per thread
    private final double alpha; // Smoothing factor for EWMA
    private final long timeWindowMillis = 15 * 60 * 1000; // 15 minutes in milliseconds

    public TpsCalculator() {
        this.alpha = 0.3; // Default smoothing factor
    }

    public TpsCalculator(double alpha) {
        this.alpha = alpha; // Set alpha (between 0 and 1)
    }

    public void recordEvent(String collectionName, String threadName) {
        statsMap.putIfAbsent(collectionName, new ConcurrentHashMap<>());
        tpsMap.putIfAbsent(collectionName, new ConcurrentHashMap<>());
        statsMap.get(collectionName).putIfAbsent(threadName, new LinkedList<>());
        tpsMap.get(collectionName).putIfAbsent(threadName, 0.0);
        
        LinkedList<Long> timestamps = statsMap.get(collectionName).get(threadName);

        long currentTime = System.currentTimeMillis();
        timestamps.add(currentTime);

        cleanOldEntries(timestamps, currentTime);
        updateTps(collectionName, threadName, timestamps);
    }

    private void updateTps(String collectionName, String threadName, LinkedList<Long> timestamps) {
        long eventCount = timestamps.size();
        double instantaneousTps = eventCount / (timeWindowMillis / 1000.0);

        double previousTps = tpsMap.get(collectionName).get(threadName);
        double smoothedTps = alpha * instantaneousTps + (1 - alpha) * previousTps;

        tpsMap.get(collectionName).put(threadName, smoothedTps);
    }

    public double calculateTps(String collectionName, String threadName) {
        return tpsMap.getOrDefault(collectionName, new ConcurrentHashMap<>()).getOrDefault(threadName, 0.0);
    }

    private void cleanOldEntries(LinkedList<Long> timestamps, long currentTime) {
        long cutOffTime = currentTime - timeWindowMillis;
        while (!timestamps.isEmpty() && timestamps.peekFirst() < cutOffTime) {
            timestamps.pollFirst();
        }
    }
}
