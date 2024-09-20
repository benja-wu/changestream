package com.example.demo.metrics;

import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class TpsCalculator {
        private final ConcurrentHashMap<String, LinkedList<Long>> statsMap = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, Double> tpsMap = new ConcurrentHashMap<>(); // Store the current TPS per
                                                                                            // thread
        private final double alpha; // Smoothing factor for EWMA
        private final long timeWindowMillis = 15 * 60 * 1000; // 15 minutes in milliseconds

        @Autowired
        public TpsCalculator(double alpha) {
                this.alpha = alpha; // Set alpha (between 0 and 1)
        }

        public void recordEvent(String threadName) {
                // Initialize stats if not present
                statsMap.putIfAbsent(threadName, new LinkedList<>());
                tpsMap.putIfAbsent(threadName, 0.0); // Initialize TPS if not present
                LinkedList<Long> timestamps = statsMap.get(threadName);

                long currentTime = System.currentTimeMillis();
                timestamps.add(currentTime); // Record the timestamp of the event

                // Clean up old timestamps (older than 15 minutes)
                cleanOldEntries(timestamps, currentTime);

                // Calculate the instantaneous TPS and update using EWMA
                updateTps(threadName, timestamps);
        }

        private void updateTps(String threadName, LinkedList<Long> timestamps) {
                // Calculate the instantaneous TPS based on events in the time window
                long eventCount = timestamps.size();
                double instantaneousTps = eventCount / (timeWindowMillis / 1000.0); // Calculate TPS based on the time
                                                                                    // window

                // Retrieve the previous TPS value
                double previousTps = tpsMap.get(threadName);

                // Apply EWMA formula: newTPS = alpha * instantaneousTPS + (1 - alpha) *
                // previousTPS
                double smoothedTps = alpha * instantaneousTps + (1 - alpha) * previousTps;

                // Update the TPS map with the new smoothed value
                tpsMap.put(threadName, smoothedTps);
        }

        public double calculateTps(String threadName) {
                // Return the smoothed TPS value; if none exists, return 0
                return tpsMap.getOrDefault(threadName, 0.0);
        }

        private void cleanOldEntries(LinkedList<Long> timestamps, long currentTime) {
                long cutOffTime = currentTime - timeWindowMillis;

                // Remove old timestamps
                while (!timestamps.isEmpty() && timestamps.peekFirst() < cutOffTime) {
                        timestamps.pollFirst(); // Remove the oldest timestamp
                }
        }
}
