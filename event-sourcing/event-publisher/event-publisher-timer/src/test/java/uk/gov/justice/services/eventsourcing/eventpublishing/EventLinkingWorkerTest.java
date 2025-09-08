package uk.gov.justice.services.eventsourcing.eventpublishing;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.eventsourcing.publishedevent.publishing.PublisherTimerConfig;
import uk.gov.justice.services.eventsourcing.util.jee.timer.StopWatchFactory;

import org.apache.commons.lang3.time.StopWatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EventLinkingWorkerTest {

    @Mock
    private EventNumberLinker eventNumberLinker;

    @Mock
    private StopWatchFactory stopWatchFactory;

    @Mock
    private PublisherTimerConfig publisherTimerConfig;

    @InjectMocks
    private EventLinkingWorker eventLinkingWorker;

    @Test
    public void shouldLinkNewEventsUntilNoMoreUnlinkedEventsAreFound() throws Exception {

        final long maxRuntimeMilliseconds = 5;
        final StopWatch stopWatch = mock(StopWatch.class);

        when(publisherTimerConfig.getTimerMaxRuntimeMilliseconds()).thenReturn(maxRuntimeMilliseconds);
        when(stopWatchFactory.createStopWatch()).thenReturn(stopWatch);

        when(eventNumberLinker.findAndAndLinkNextUnlinkedEvent()).thenReturn(true, true, false);
        when(stopWatch.getTime()).thenReturn(1L, 2L);

        eventLinkingWorker.findAndLinkUnlinkedEvents();

        verify(eventNumberLinker, times(3)).findAndAndLinkNextUnlinkedEvent();
    }

    @Test
    public void shouldContinueLinkingEventsUntilMaxTimeIsExceeded() throws Exception {

        final long maxRuntimeMilliseconds = 5;
        final StopWatch stopWatch = mock(StopWatch.class);

        when(publisherTimerConfig.getTimerMaxRuntimeMilliseconds()).thenReturn(maxRuntimeMilliseconds);
        when(stopWatchFactory.createStopWatch()).thenReturn(stopWatch);

        when(eventNumberLinker.findAndAndLinkNextUnlinkedEvent()).thenReturn(true);
        when(stopWatch.getTime()).thenReturn(1L, 2L, 3L, 4L, 5L, 6L);

        eventLinkingWorker.findAndLinkUnlinkedEvents();

        final InOrder inOrder = inOrder(stopWatch, eventNumberLinker);

        inOrder.verify(stopWatch).start();
        inOrder.verify(eventNumberLinker, times(6)).findAndAndLinkNextUnlinkedEvent();
    }
}