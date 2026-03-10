package uk.gov.justice.services.event.sourcing.subscription.manager.task;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventProcessingStatus.EVENT_FOUND;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventProcessingStatus.EVENT_NOT_FOUND;

import uk.gov.justice.services.event.sourcing.subscription.error.StreamProcessingException;
import uk.gov.justice.services.event.sourcing.subscription.manager.StreamEventProcessor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class StreamProcessingWorkerBeanTest {

    @Mock
    private StreamEventProcessor streamEventProcessor;

    @Mock
    private PollerCircuitBreaker pollerCircuitBreaker;

    @Mock
    private Logger logger;

    @InjectMocks
    private StreamProcessingWorkerBean streamProcessingWorkerBean;

    @Test
    public void shouldProcessEventsUntilIdle() {
        final String source = "test-source";
        final String component = "test-component";

        when(streamEventProcessor.processSingleEvent(source, component))
                .thenReturn(EVENT_FOUND)
                .thenReturn(EVENT_FOUND)
                .thenReturn(EVENT_NOT_FOUND);

        streamProcessingWorkerBean.processUntilIdle(source, component);

        verify(streamEventProcessor, times(3)).processSingleEvent(source, component);
    }

    @Test
    public void shouldStopImmediatelyWhenNoEventsFound() {
        final String source = "test-source";
        final String component = "test-component";

        when(streamEventProcessor.processSingleEvent(source, component))
                .thenReturn(EVENT_NOT_FOUND);

        streamProcessingWorkerBean.processUntilIdle(source, component);

        verify(streamEventProcessor, times(1)).processSingleEvent(source, component);
    }

    @Test
    public void shouldLogErrorWhenExceptionThrown() {
        final String source = "test-source";
        final String component = "test-component";
        final RuntimeException exception = new RuntimeException("Processing failed");

        doThrow(exception).when(streamEventProcessor).processSingleEvent(source, component);

        streamProcessingWorkerBean.processUntilIdle(source, component);

        verify(logger).error("Error processing stream events for source: {}, component: {}", source, component, exception);
    }

    @Test
    public void shouldSkipProcessingWhenCircuitBreakerIsOpen() {
        final String source = "test-source";
        final String component = "test-component";

        when(pollerCircuitBreaker.isOpen(source, component)).thenReturn(true);

        streamProcessingWorkerBean.processUntilIdle(source, component);

        verify(streamEventProcessor, never()).processSingleEvent(any(), any());
        verify(pollerCircuitBreaker, never()).recordSuccess(any(), any());
        verify(pollerCircuitBreaker, never()).recordFailure(any(), any());
        verify(logger).warn("Circuit breaker open, skipping processing for source: {}, component: {}",
                source, component);
    }

    @Test
    public void shouldRecordSuccessAfterLoopCompletesNormally() {
        final String source = "test-source";
        final String component = "test-component";

        when(streamEventProcessor.processSingleEvent(source, component)).thenReturn(EVENT_NOT_FOUND);

        streamProcessingWorkerBean.processUntilIdle(source, component);

        verify(pollerCircuitBreaker).recordSuccess(source, component);
        verify(pollerCircuitBreaker, never()).recordFailure(any(), any());
    }

    @Test
    public void shouldRecordFailureWhenGeneralExceptionThrown() {
        final String source = "test-source";
        final String component = "test-component";
        final RuntimeException exception = new RuntimeException("Connection error");

        doThrow(exception).when(streamEventProcessor).processSingleEvent(source, component);

        streamProcessingWorkerBean.processUntilIdle(source, component);

        verify(pollerCircuitBreaker).recordFailure(source, component);
        verify(pollerCircuitBreaker, never()).recordSuccess(any(), any());
        verify(logger).error("Error processing stream events for source: {}, component: {}", source, component, exception);
    }
}
