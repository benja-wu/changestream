// DemoApplicationTest.java
package com.example.demo;

import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.example.demo.service.EventProcessingMediator;

@SpringBootTest(classes = DemoApplication.class)
@ExtendWith(SpringExtension.class)
class DemoApplicationTest {

        @MockBean
        private EventProcessingMediator mediator; // MockBean to integrate with Spring context

        @InjectMocks
        private DemoApplication demoApplication;

        @BeforeEach
        void setUp() {
                MockitoAnnotations.openMocks(this);
                // Reset the mock to ensure no previous invocations affect the current test
                reset(mediator);
        }

        @Test
        void testStartChangeStreamListener() {
                // Call the method that triggers the listener startup
                demoApplication.startChangeStreamListener();

                // Verify that the changeStreamProcessWithRetry method was called exactly once
                verify(mediator, times(1)).changeStreamProcessWithRetry();
        }
}
