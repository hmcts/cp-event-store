package uk.gov.justice.services.event.sourcing.subscription.manager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class StreamProcessingSubscriptionManagerTest {

    @Mock
    private StreamEventProcessor streamEventProcessor;

    @InjectMocks
    private StreamProcessingSubscriptionManager streamProcessingSubscriptionManager;

    @Test
    public void shouldProcessSingleEventAndBreakWhenNoMoreEventsToProcess() {

        final String source = "some-source";
        final String component = "some-component";

        when(streamEventProcessor.processSingleEvent(source, component)).thenReturn(false);

        streamProcessingSubscriptionManager.process(source, component, null);

        verify(streamEventProcessor).processSingleEvent(source, component);
    }

    @Test
    public void shouldProcessMultipleEventsUntilNoMoreEventsAvailable() {

        final String source = "some-source";
        final String component = "some-component";

        when(streamEventProcessor.processSingleEvent(source, component))
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(false);

        streamProcessingSubscriptionManager.process(source, component, () -> true);

        final InOrder inOrder = inOrder(streamEventProcessor);

        inOrder.verify(streamEventProcessor, times(4)).processSingleEvent(source, component);
    }

    @Test
    public void shouldNotProcessAnyEventsWhenFirstCallReturnsFalse() {

        final String source = "some-source";
        final String component = "some-component";

        when(streamEventProcessor.processSingleEvent(source, component)).thenReturn(false);

        streamProcessingSubscriptionManager.process(source, component, null);

        verify(streamEventProcessor).processSingleEvent(source, component);
    }
}
