package uk.gov.justice.services.eventsourcing.eventpublishing;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EventLinkingWorkerTest {

    @Mock
    private EventNumberLinker eventNumberLinker;

    @InjectMocks
    private EventLinkingWorker eventLinkingWorker;

    @Test
    public void shouldLinkBatchesUntilNoneLinked() throws Exception {

        final SufficientTimeRemainingCalculator sufficientTimeRemainingCalculator = mock(SufficientTimeRemainingCalculator.class);

        when(sufficientTimeRemainingCalculator.hasSufficientProcessingTimeRemaining()).thenReturn(true);
        when(eventNumberLinker.findAndLinkEventsInBatch()).thenReturn(10, 5, 0);

        eventLinkingWorker.linkNewEvents(sufficientTimeRemainingCalculator);

        verify(eventNumberLinker, times(3)).findAndLinkEventsInBatch();
    }

    @Test
    public void shouldStopLinkingWhenTimeIsExceeded() throws Exception {

        final SufficientTimeRemainingCalculator sufficientTimeRemainingCalculator = mock(SufficientTimeRemainingCalculator.class);

        when(sufficientTimeRemainingCalculator.hasSufficientProcessingTimeRemaining()).thenReturn(true, true, false);
        when(eventNumberLinker.findAndLinkEventsInBatch()).thenReturn(10);

        eventLinkingWorker.linkNewEvents(sufficientTimeRemainingCalculator);

        verify(eventNumberLinker, times(2)).findAndLinkEventsInBatch();
    }

    @Test
    public void shouldNotProcessIfNoSufficientTimeRemaining() throws Exception {

        final SufficientTimeRemainingCalculator sufficientTimeRemainingCalculator = mock(SufficientTimeRemainingCalculator.class);

        when(sufficientTimeRemainingCalculator.hasSufficientProcessingTimeRemaining()).thenReturn(false);

        eventLinkingWorker.linkNewEvents(sufficientTimeRemainingCalculator);

        verify(eventNumberLinker, never()).findAndLinkEventsInBatch();
    }
}