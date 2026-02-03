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
import uk.gov.justice.services.eventsourcing.util.jee.timer.SufficientTimeRemainingCalculator;

@ExtendWith(MockitoExtension.class)
public class EventLinkingWorkerTest {

    @Mock
    private EventNumberLinker eventNumberLinker;

    @InjectMocks
    private EventLinkingWorker eventLinkingWorker;

    @Test
    public void shouldLinkNewEventsUntilNoMoreUnlinkedEventsAreFound() throws Exception {

        final SufficientTimeRemainingCalculator sufficientTimeRemainingCalculator = mock(SufficientTimeRemainingCalculator.class);

        when(sufficientTimeRemainingCalculator.hasSufficientProcessingTimeRemaining()).thenReturn(true);
        when(eventNumberLinker.findAndAndLinkNextUnlinkedEvent()).thenReturn(true, true, false);

        eventLinkingWorker.linkNewEvents(sufficientTimeRemainingCalculator);

        verify(eventNumberLinker, times(3)).findAndAndLinkNextUnlinkedEvent();
    }

    @Test
    public void shouldContinueLinkingEventsUntilMaxTimeIsExceeded() throws Exception {

        final SufficientTimeRemainingCalculator sufficientTimeRemainingCalculator = mock(SufficientTimeRemainingCalculator.class);

        when(sufficientTimeRemainingCalculator.hasSufficientProcessingTimeRemaining()).thenReturn(true, true, false);
        when(eventNumberLinker.findAndAndLinkNextUnlinkedEvent()).thenReturn(true);

        eventLinkingWorker.linkNewEvents(sufficientTimeRemainingCalculator);

        verify(eventNumberLinker, times(2)).findAndAndLinkNextUnlinkedEvent();
    }

    @Test
    public void shouldNotProcessNextEventIfNoSufficientProcessingTimeRemaining() throws Exception {

        final SufficientTimeRemainingCalculator sufficientTimeRemainingCalculator = mock(SufficientTimeRemainingCalculator.class);

        when(sufficientTimeRemainingCalculator.hasSufficientProcessingTimeRemaining()).thenReturn(false);

        eventLinkingWorker.linkNewEvents(sufficientTimeRemainingCalculator);

        verify(eventNumberLinker, never()).findAndAndLinkNextUnlinkedEvent();
    }
}