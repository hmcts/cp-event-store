package uk.gov.justice.services.event.sourcing.subscription.manager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.services.eventsourcing.util.jee.timer.SufficientTimeRemainingCalculator;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventProcessingStatus.EVENT_FOUND;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventProcessingStatus.EVENT_NOT_FOUND;

@ExtendWith(MockitoExtension.class)
public class StreamProcessingSubscriptionManagerTest {

    @Mock
    private StreamEventProcessor streamEventProcessor;

    @Mock
    private SufficientTimeRemainingCalculator sufficientTimeRemainingCalculator;

    @InjectMocks
    private StreamProcessingSubscriptionManager streamProcessingSubscriptionManager;

    @Test
    public void shouldProcessSingleEventAndBreakWhenNoMoreEventsToProcess() {

        final String source = "some-source";
        final String component = "some-component";

        when(streamEventProcessor.processSingleEvent(source, component)).thenReturn(EVENT_NOT_FOUND);

        streamProcessingSubscriptionManager.process(source, component, sufficientTimeRemainingCalculator);

        verify(streamEventProcessor).processSingleEvent(source, component);
    }

    @Test
    public void shouldProcessMultipleEventsUntilNoMoreEventsAvailable() {

        final String source = "some-source";
        final String component = "some-component";

        when(streamEventProcessor.processSingleEvent(source, component))
                .thenReturn(EVENT_FOUND)
                .thenReturn(EVENT_FOUND)
                .thenReturn(EVENT_FOUND)
                .thenReturn(EVENT_NOT_FOUND);
        when(sufficientTimeRemainingCalculator.hasSufficientProcessingTimeRemaining()).thenReturn(true);

        streamProcessingSubscriptionManager.process(source, component, sufficientTimeRemainingCalculator);

        final InOrder inOrder = inOrder(streamEventProcessor);

        inOrder.verify(streamEventProcessor, times(4)).processSingleEvent(source, component);
    }

    @Test
    public void shouldNotProcessAnyEventsWhenFirstCallReturnsEventNotFound() {

        final String source = "some-source";
        final String component = "some-component";

        when(streamEventProcessor.processSingleEvent(source, component)).thenReturn(EVENT_NOT_FOUND);

        streamProcessingSubscriptionManager.process(source, component, sufficientTimeRemainingCalculator);

        verify(streamEventProcessor).processSingleEvent(source, component);
    }

    @Test
    public void shouldStopProcessingWhenNoSufficientTimeRemaining() {

        final String source = "some-source";
        final String component = "some-component";

        when(streamEventProcessor.processSingleEvent(source, component)).thenReturn(EVENT_FOUND);
        when(sufficientTimeRemainingCalculator.hasSufficientProcessingTimeRemaining()).thenReturn(false);

        streamProcessingSubscriptionManager.process(source, component, sufficientTimeRemainingCalculator);

        verify(streamEventProcessor).processSingleEvent(source, component);
        verify(sufficientTimeRemainingCalculator).hasSufficientProcessingTimeRemaining();
    }
}
