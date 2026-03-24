package uk.gov.justice.services.eventsourcing.eventpublishing;

import static java.util.Collections.emptyList;
import static java.util.UUID.randomUUID;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.EventDetailsToLink;
import uk.gov.justice.services.eventsourcing.util.jee.timer.SufficientTimeRemainingCalculator;

@ExtendWith(MockitoExtension.class)
public class EventLinkingWorkerTest {

    @Mock
    private EventNumberLinker eventNumberLinker;

    @InjectMocks
    private EventLinkingWorker eventLinkingWorker;

    @Test
    public void shouldLinkBatchesUntilNoneLinked() throws Exception {

        final SufficientTimeRemainingCalculator sufficientTimeRemainingCalculator = mock(SufficientTimeRemainingCalculator.class);
        final List<EventDetailsToLink> batch = List.of(new EventDetailsToLink(randomUUID(), randomUUID(), 1));

        when(sufficientTimeRemainingCalculator.hasSufficientProcessingTimeRemaining()).thenReturn(true);
        when(eventNumberLinker.findAndLinkEventsInBatch()).thenReturn(batch, batch, emptyList());

        eventLinkingWorker.linkNewEvents(sufficientTimeRemainingCalculator);

        verify(eventNumberLinker, times(3)).findAndLinkEventsInBatch();
    }

    @Test
    public void shouldStopLinkingWhenTimeIsExceeded() throws Exception {

        final SufficientTimeRemainingCalculator sufficientTimeRemainingCalculator = mock(SufficientTimeRemainingCalculator.class);
        final List<EventDetailsToLink> batch = List.of(new EventDetailsToLink(randomUUID(), randomUUID(), 1));

        when(sufficientTimeRemainingCalculator.hasSufficientProcessingTimeRemaining()).thenReturn(true, true, false);
        when(eventNumberLinker.findAndLinkEventsInBatch()).thenReturn(batch);

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