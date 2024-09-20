
package com.example.demo;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.example.demo.metrics.TpsCalculator;

class TpsCalculatorTest {

        private TpsCalculator tpsCalculator;

        @BeforeEach
        void setUp() {
                tpsCalculator = new TpsCalculator(0.5); // Alpha set to 0.5 for EWMA
        }

        @Test
        void testCalculateTps() throws InterruptedException {
                String threadName = "test-thread";

                // Simulate recording of events
                tpsCalculator.recordEvent(threadName);
                TimeUnit.MILLISECONDS.sleep(10);
                tpsCalculator.recordEvent(threadName);

                // Calculate TPS
                double tps = tpsCalculator.calculateTps(threadName);

                // Assert TPS value is as expected (depends on the time window and alpha)
                assertEquals(0.008, tps, 0.1); // Adjust tolerance as needed
        }

        @Test
        void testCleanOldEntries() throws InterruptedException {
                String threadName = "test-thread";

                // Simulate events, including older ones
                tpsCalculator.recordEvent(threadName);
                TimeUnit.MILLISECONDS.sleep(10);
                tpsCalculator.recordEvent(threadName);
                TimeUnit.MILLISECONDS.sleep(15 * 60);

                // Clean old entries and check TPS
                double tps = tpsCalculator.calculateTps(threadName);

                assertEquals(0.0, tps, 0.1); // After 15 minutes, TPS should be 0
        }
}
