package uk.gov.justice.services.event.sourcing.subscription.manager.task;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.doThrow;
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
    private Logger logger;

    @InjectMocks
    private StreamProcessingWorkerBean streamProcessingWorkerBean;

    @Test
    public void shouldProcessEventsUntilIdleAndReturnTrue() {
        final String source = "test-source";
        final String component = "test-component";

        when(streamEventProcessor.processSingleEvent(source, component))
                .thenReturn(EVENT_FOUND)
                .thenReturn(EVENT_FOUND)
                .thenReturn(EVENT_NOT_FOUND);

        final boolean result = streamProcessingWorkerBean.processUntilIdle(source, component);

        assertThat(result, is(true));
        verify(streamEventProcessor, times(3)).processSingleEvent(source, component);
    }

    @Test
    public void shouldReturnFalseWhenNoEventsFound() {
        final String source = "test-source";
        final String component = "test-component";

        when(streamEventProcessor.processSingleEvent(source, component))
                .thenReturn(EVENT_NOT_FOUND);

        final boolean result = streamProcessingWorkerBean.processUntilIdle(source, component);

        assertThat(result, is(false));
        verify(streamEventProcessor, times(1)).processSingleEvent(source, component);
    }

    @Test
    public void shouldLogErrorAndReturnFalseWhenExceptionThrown() {
        final String source = "test-source";
        final String component = "test-component";
        final RuntimeException exception = new RuntimeException("Processing failed");

        doThrow(exception).when(streamEventProcessor).processSingleEvent(source, component);

        final boolean result = streamProcessingWorkerBean.processUntilIdle(source, component);

        assertThat(result, is(false));
        verify(logger).error("Error processing stream events for source: {}, component: {}", source, component, exception);
    }

    @Test
    public void shouldLogErrorAndReturnTrueWhenExceptionThrownAfterSomeEventsProcessed() {
        final String source = "test-source";
        final String component = "test-component";
        final RuntimeException exception = new RuntimeException("Processing failed");

        when(streamEventProcessor.processSingleEvent(source, component))
                .thenReturn(EVENT_FOUND)
                .thenThrow(exception);

        final boolean result = streamProcessingWorkerBean.processUntilIdle(source, component);

        assertThat(result, is(true));
        verify(logger).error("Error processing stream events for source: {}, component: {}", source, component, exception);
    }

    @Test
    public void shouldReturnTrueWhenStreamProcessingExceptionThrown() {
        final String source = "test-source";
        final String component = "test-component";
        final StreamProcessingException exception = new StreamProcessingException("Events not linked yet");

        doThrow(exception).when(streamEventProcessor).processSingleEvent(source, component);

        final boolean result = streamProcessingWorkerBean.processUntilIdle(source, component);

        assertThat(result, is(true));
        verify(logger).warn("Stream has pending events not yet available for source: {}, component: {}: {}",
                source, component, "Events not linked yet");
    }
}
