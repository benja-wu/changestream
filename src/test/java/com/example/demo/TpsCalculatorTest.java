package com.example.demo;

import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

import com.example.demo.metrics.*;;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TpsCalculatorTest {

    private TpsCalculator tpsCalculator;
    private final String collectionName = "testCollection";
    private final String threadName = "test-thread";

    @BeforeEach
    void setUp() {
        tpsCalculator = new TpsCalculator(0.5); // Alpha set to 0.5 for EWMA
    }

    @Test
    void testCalculateTps() throws Exception {
        // Mock timestamps directly to avoid time-dependent behavior
        setMockedTimestamps(collectionName, threadName, System.currentTimeMillis(), 2);

        // Calculate TPS
        double tps = tpsCalculator.calculateTps(collectionName, threadName);

        // Assert TPS value within a reasonable tolerance
        assertEquals(2.0 / (15 * 60), tps, 0.01); // TPS formula: count / window_seconds
    }

    @Test
    void testCleanOldEntries() throws Exception {
        // Simulate old events (15+ minutes ago)
        long oldTime = System.currentTimeMillis() - (16 * 60 * 1000);
        setMockedTimestamps(collectionName, threadName, oldTime, 2);

        // Ensure the events are cleaned and TPS is 0
        double tps = tpsCalculator.calculateTps(collectionName, threadName);
        assertEquals(0.0, tps, 0.01);
    }

    /** ✅ Helper method to inject mocked timestamps into TpsCalculator **/
    private void setMockedTimestamps(String collection, String thread, long startTime, int count) throws Exception {
        // Use reflection to manipulate the private statsMap field
        Field statsMapField = TpsCalculator.class.getDeclaredField("statsMap");
        statsMapField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentHashMap<String, ConcurrentHashMap<String, LinkedList<Long>>> statsMap =
                (ConcurrentHashMap<String, ConcurrentHashMap<String, LinkedList<Long>>>) statsMapField.get(tpsCalculator);

        // Ensure nested maps exist
        statsMap.putIfAbsent(collection, new ConcurrentHashMap<>());
        statsMap.get(collection).putIfAbsent(thread, new LinkedList<>());

        LinkedList<Long> timestamps = statsMap.get(collection).get(thread);
        timestamps.clear(); // Clear previous entries

        for (int i = 0; i < count; i++) {
            timestamps.add(startTime + (i * 1000)); // Add timestamps 1s apart
        }
    }
}
