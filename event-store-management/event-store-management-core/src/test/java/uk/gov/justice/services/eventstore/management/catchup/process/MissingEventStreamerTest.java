package uk.gov.justice.services.eventstore.management.catchup.process;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.event.sourcing.subscription.manager.LinkedEventSourceProvider;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventsourcing.source.api.service.core.LinkedEventSource;
import uk.gov.justice.services.eventsourcing.source.api.streams.MissingEventRange;
import uk.gov.justice.services.subscription.ProcessedEventTrackingService;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class MissingEventStreamerTest {

    @Mock
    private LinkedEventSourceProvider linkedEventSourceProvider;

    @Mock
    private ProcessedEventTrackingService processedEventTrackingService;

    @InjectMocks
    private MissingEventStreamer missingEventStreamer;

    @Test
    public void shouldFindTheRangesOfMissingEventsAndStreamThemAsPublishedEvents() throws Exception {

        final String componentName = "EVENT_LISTENER";
        final String eventSourceName = "event source name";
        final Long highestPublishedEventNumber = 23L;
        final Long runFromEventNumber = 4L;

        final LinkedEventSource linkedEventSource = mock(LinkedEventSource.class);

        final MissingEventRange missingEventRange_1 = mock(MissingEventRange.class);
        final MissingEventRange missingEventRange_2 = mock(MissingEventRange.class);

        final Stream<MissingEventRange> missingEventRangeStream = Stream.of(missingEventRange_1, missingEventRange_2);

        final LinkedEvent linkedEvent_2 = mock(LinkedEvent.class);
        final LinkedEvent linkedEvent_3 = mock(LinkedEvent.class);

        final LinkedEvent linkedEvent_7 = mock(LinkedEvent.class);
        final LinkedEvent linkedEvent_8 = mock(LinkedEvent.class);

        final Stream<LinkedEvent> publishedEventStream_1 = Stream.of(linkedEvent_2, linkedEvent_3);
        final Stream<LinkedEvent> publishedEventStream_2 = Stream.of(linkedEvent_7, linkedEvent_8);


        when(linkedEventSourceProvider.getLinkedEventSource(eventSourceName)).thenReturn(linkedEventSource);
        when(linkedEventSource.getHighestPublishedEventNumber()).thenReturn(highestPublishedEventNumber);
        when(processedEventTrackingService.getAllMissingEvents(eventSourceName, componentName, runFromEventNumber, highestPublishedEventNumber)).thenReturn(missingEventRangeStream);
        when(linkedEventSource.findEventRange(missingEventRange_1)).thenReturn(publishedEventStream_1);
        when(linkedEventSource.findEventRange(missingEventRange_2)).thenReturn(publishedEventStream_2);

        final List<LinkedEvent> missingEvents = missingEventStreamer.getMissingEvents(
                        eventSourceName,
                        componentName,
                        runFromEventNumber)
                .toList();

        assertThat(missingEvents.size(), is(4));
        assertThat(missingEvents.get(0), is(linkedEvent_2));
        assertThat(missingEvents.get(1), is(linkedEvent_3));
        assertThat(missingEvents.get(2), is(linkedEvent_7));
        assertThat(missingEvents.get(3), is(linkedEvent_8));
    }
}
