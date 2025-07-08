package uk.gov.justice.services.event.sourcing.subscription.manager;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventOrderingStatus.EVENT_ALREADY_PROCESSED;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventOrderingStatus.EVENT_CORRECTLY_ORDERED;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventOrderingStatus.EVENT_OUT_OF_ORDER;

import uk.gov.justice.services.eventsourcing.util.messaging.EventSourceNameCalculator;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.metrics.micrometer.counters.MicrometerMetricsCounters;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class NewSubscriptionManagerDelegateTest {

    @Mock
    private EventBufferAwareSubscriptionEventProcessor eventBufferAwareSubscriptionEventProcessor;

    @Mock
    private StreamStatusService streamStatusService;

    @Mock
    private MicrometerMetricsCounters micrometerMetricsCounters;

    @Mock
    private EventSourceNameCalculator eventSourceNameCalculator;

    @InjectMocks
    private NewSubscriptionManagerDelegate newSubscriptionManagerDelegate;

    @Test
    public void shouldHandleStreamStatusUpdatesAndSendEventToEventBufferProcessorIfEventInCorrectOrder() throws Exception {

        final JsonEnvelope incomingJsonEnvelope = mock(JsonEnvelope.class);
        final String componentName = "some-component-name";
        final String source = "some-source";

        when(eventSourceNameCalculator.getSource(incomingJsonEnvelope)).thenReturn(source);
        when(streamStatusService.handleStreamStatusUpdates(
                incomingJsonEnvelope,
                componentName)).thenReturn(EVENT_CORRECTLY_ORDERED);

        newSubscriptionManagerDelegate.process(incomingJsonEnvelope, componentName);

        verify(eventSourceNameCalculator).getSource(incomingJsonEnvelope);
        verify(micrometerMetricsCounters).incrementEventsReceivedCount(source,componentName);
        verify(eventBufferAwareSubscriptionEventProcessor).process(incomingJsonEnvelope, componentName);
    }

    @Test
    public void shouldHandleStreamStatusUpdatesAndDoNothingElseIfEventOutOfOrder() throws Exception {

        final JsonEnvelope incomingJsonEnvelope = mock(JsonEnvelope.class);
        final String componentName = "some-component-name";
        final String source = "some-source";

        when(eventSourceNameCalculator.getSource(incomingJsonEnvelope)).thenReturn(source);
        when(streamStatusService.handleStreamStatusUpdates(
                incomingJsonEnvelope,
                componentName)).thenReturn(EVENT_OUT_OF_ORDER);

        newSubscriptionManagerDelegate.process(incomingJsonEnvelope, componentName);

        verify(eventSourceNameCalculator).getSource(incomingJsonEnvelope);
        verify(micrometerMetricsCounters).incrementEventsReceivedCount(source,componentName);
        verify(eventBufferAwareSubscriptionEventProcessor, never()).process(incomingJsonEnvelope, componentName);
    }

    @Test
    public void shouldHandleStreamStatusUpdatesAndDoNothingElseIfEventAlreadyProcessed() throws Exception {

        final JsonEnvelope incomingJsonEnvelope = mock(JsonEnvelope.class);
        final String componentName = "some-component-name";
        final String source = "some-source";

        when(eventSourceNameCalculator.getSource(incomingJsonEnvelope)).thenReturn(source);
        when(streamStatusService.handleStreamStatusUpdates(
                incomingJsonEnvelope,
                componentName)).thenReturn(EVENT_ALREADY_PROCESSED);

        newSubscriptionManagerDelegate.process(incomingJsonEnvelope, componentName);

        verify(eventSourceNameCalculator).getSource(incomingJsonEnvelope);
        verify(micrometerMetricsCounters).incrementEventsReceivedCount(source,componentName);
        verify(eventBufferAwareSubscriptionEventProcessor, never()).process(incomingJsonEnvelope, componentName);
    }
}
