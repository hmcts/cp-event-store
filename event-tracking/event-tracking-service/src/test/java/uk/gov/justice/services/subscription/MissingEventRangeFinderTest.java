package uk.gov.justice.services.subscription;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Optional.of;
import static java.util.stream.Stream.empty;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.subscription.ProcessedEventBuilder.processedEventTrackItem;

import uk.gov.justice.services.eventsourcing.source.api.streams.MissingEventRange;
import uk.gov.justice.services.test.utils.common.stream.StreamCloseSpy;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class MissingEventRangeFinderTest {

    @Mock
    private ProcessedEventTrackingRepository processedEventTrackingRepository;

    @Mock
    private ProcessedEventStreamer processedEventStreamer;

    @InjectMocks
    private MissingEventRangeFinder missingEventRangeFinder;

    @Test
    public void shouldGetTheListOfAllMissingEvents() throws Exception {

        final String source = "example-context";
        final String componentName = "EVENT_LISTENER";
        final Long runFromEventNumber = 23L;
        final Long highestPublishedEventNumber = 267L;
        final Long highestExclusiveEventNumber = highestPublishedEventNumber + 1;

        final List<ProcessedEvent> processedEvents = asList(
                processedEventTrackItem()
                        .withEventNumber(7)
                        .withPreviousEventNumber(6)
                        .withSource(source)
                        .withComponentName(componentName)
                        .build(),
                processedEventTrackItem()
                        .withEventNumber(3)
                        .withPreviousEventNumber(2)
                        .withSource(source)
                        .withComponentName(componentName)
                        .build(),
                processedEventTrackItem()
                        .withEventNumber(2)
                        .withPreviousEventNumber(1)
                        .withSource(source)
                        .withComponentName(componentName)
                        .build(),
                processedEventTrackItem()
                        .withEventNumber(1)
                        .withPreviousEventNumber(0)
                        .withSource(source)
                        .withComponentName(componentName)
                        .build()
        );

        final ProcessedEvent latestProcessedEvent = processedEventTrackItem()
                .withEventNumber(7)
                .withPreviousEventNumber(6)
                .withSource(source)
                .withComponentName(componentName)
                .build();

        final StreamCloseSpy streamCloseSpy = new StreamCloseSpy();
        final Stream<ProcessedEvent> processedEventTrackItemStream = processedEvents.stream().onClose(streamCloseSpy);

        when(processedEventTrackingRepository.getLatestProcessedEvent(source, componentName)).thenReturn(of(latestProcessedEvent));
        when(processedEventStreamer.getProcessedEventStream(source, componentName, runFromEventNumber)).thenReturn(processedEventTrackItemStream);

        final List<MissingEventRange> missingEventRanges = missingEventRangeFinder.getRangesOfMissingEvents(
                source,
                componentName,
                runFromEventNumber,
                highestPublishedEventNumber);

        assertThat(missingEventRanges.size(), is(2));

        assertThat(missingEventRanges.get(0).getMissingEventFrom(), is(4L));
        assertThat(missingEventRanges.get(0).getMissingEventTo(), is(7L));
        assertThat(missingEventRanges.get(1).getMissingEventFrom(), is(8L));
        assertThat(missingEventRanges.get(1).getMissingEventTo(), is(highestExclusiveEventNumber));

        assertThat(streamCloseSpy.streamClosed(), is(true));
    }

    @Test
    public void shouldHandleMissingEventsFromZero() throws Exception {

        final String source = "example-context";
        final String componentName = "EVENT_LISTENER";
        final Long runFromEventNumber = 23L;
        final Long highestPublishedEventNumber = 253L;
        final Long highestExclusiveEventNumber = highestPublishedEventNumber + 1;

        final List<ProcessedEvent> processedEvents = asList(
                processedEventTrackItem()
                        .withPreviousEventNumber(24)
                        .withEventNumber(25)
                        .withSource(source)
                        .withComponentName(componentName)
                        .build(),
                processedEventTrackItem()
                        .withPreviousEventNumber(23)
                        .withEventNumber(24)
                        .withSource(source)
                        .withComponentName(componentName)
                        .build(),
                processedEventTrackItem()
                        .withPreviousEventNumber(19)
                        .withEventNumber(20)
                        .withSource(source)
                        .withComponentName(componentName)
                        .build()
        );

        final StreamCloseSpy streamCloseSpy = new StreamCloseSpy();
        final Stream<ProcessedEvent> processedEventTrackItemStream = processedEvents.stream().onClose(streamCloseSpy);

        final ProcessedEvent processedEvent = processedEventTrackItem()
                .withPreviousEventNumber(24)
                .withEventNumber(25)
                .withSource(source)
                .withComponentName(componentName)
                .build();

        when(processedEventTrackingRepository.getLatestProcessedEvent(source, componentName)).thenReturn(of(processedEvent));
        when(processedEventStreamer.getProcessedEventStream(source, componentName, runFromEventNumber)).thenReturn(processedEventTrackItemStream);

        final List<MissingEventRange> missingEventRanges = missingEventRangeFinder.getRangesOfMissingEvents(
                source,
                componentName,
                runFromEventNumber,
                highestPublishedEventNumber);

        assertThat(missingEventRanges.size(), is(3));

        assertThat(missingEventRanges.get(0).getMissingEventFrom(), is(1L));
        assertThat(missingEventRanges.get(0).getMissingEventTo(), is(20L));
        assertThat(missingEventRanges.get(1).getMissingEventFrom(), is(21L));
        assertThat(missingEventRanges.get(1).getMissingEventTo(), is(24L));
        assertThat(missingEventRanges.get(2).getMissingEventFrom(), is(26L));
        assertThat(missingEventRanges.get(2).getMissingEventTo(), is(highestExclusiveEventNumber));

        assertThat(streamCloseSpy.streamClosed(), is(true));
    }

    @Test
    public void shouldReturnRangeOfOneToMaxLongIfNoEventsFound() throws Exception {

        final String source = "example-context";
        final String componentName = "EVENT_LISTENER";
        final Long runFromEventNumber = 23L;
        final Long highestPublishedEventNumber = 2134L;
        final Long highestExclusiveEventNumber = highestPublishedEventNumber + 1;

        when(processedEventTrackingRepository.getLatestProcessedEvent(source, componentName)).thenReturn(Optional.empty());
        when(processedEventStreamer.getProcessedEventStream(source, componentName, runFromEventNumber)).thenReturn(empty());

        final List<MissingEventRange> missingEventRanges = missingEventRangeFinder.getRangesOfMissingEvents(
                source,
                componentName,
                runFromEventNumber,
                highestPublishedEventNumber);

        assertThat(missingEventRanges.size(), is(1));

        assertThat(missingEventRanges.get(0).getMissingEventFrom(), is(1L));
        assertThat(missingEventRanges.get(0).getMissingEventTo(), is(highestExclusiveEventNumber));
    }

    @Test
    public void shouldReturnNoMissingEventsIfNoEventsAreActuallyMissing() throws Exception {

        final String source = "example-context";
        final String componentName = "EVENT_LISTENER";
        final Long runFromEventNumber = 23L;
        final Long highestPublishedEventNumber = 23423L;
        final Long highestExclusiveEventNumber = highestPublishedEventNumber + 1;

        final List<ProcessedEvent> processedEvents = asList(
                processedEventTrackItem()
                        .withEventNumber(4)
                        .withPreviousEventNumber(3)
                        .withSource(source)
                        .withComponentName(componentName)
                        .build(),
                processedEventTrackItem()
                        .withEventNumber(3)
                        .withPreviousEventNumber(2)
                        .withSource(source)
                        .withComponentName(componentName)
                        .build(),
                processedEventTrackItem()
                        .withEventNumber(2)
                        .withPreviousEventNumber(1)
                        .withSource(source)
                        .withComponentName(componentName)
                        .build(),
                processedEventTrackItem()
                        .withEventNumber(1)
                        .withPreviousEventNumber(0)
                        .withSource(source)
                        .withComponentName(componentName)
                        .build()
        );

        final ProcessedEvent latestProcessedEvent = processedEventTrackItem()
                .withEventNumber(4)
                .withPreviousEventNumber(3)
                .withSource(source)
                .withComponentName(componentName)
                .build();

        final StreamCloseSpy streamCloseSpy = new StreamCloseSpy();
        final Stream<ProcessedEvent> processedEventTrackItemStream = processedEvents.stream().onClose(streamCloseSpy);

        when(processedEventTrackingRepository.getLatestProcessedEvent(source, componentName)).thenReturn(of(latestProcessedEvent));
        when(processedEventStreamer.getProcessedEventStream(source, componentName, runFromEventNumber)).thenReturn(processedEventTrackItemStream);

        final List<MissingEventRange> missingEventRanges = missingEventRangeFinder.getRangesOfMissingEvents(
                source,
                componentName,
                runFromEventNumber,
                highestPublishedEventNumber);
        assertThat(missingEventRanges.size(), is(1));

        assertThat(missingEventRanges.get(0).getMissingEventFrom(), is(5L));
        assertThat(missingEventRanges.get(0).getMissingEventTo(), is(highestExclusiveEventNumber));

        assertThat(streamCloseSpy.streamClosed(), is(true));
    }

    @Test
    public void shouldReturnNoMissingEventsIfOnlyOneEventExists() throws Exception {

        final String source = "example-context";
        final String componentName = "EVENT_LISTENER";
        final Long runFromEventNumber = 23L;
        final Long highestPublishedEventNumber = 1234L;
        final Long highestExclusiveEventNumber = highestPublishedEventNumber + 1;

        final List<ProcessedEvent> processedEvents = singletonList(
                processedEventTrackItem()
                        .withEventNumber(1)
                        .withPreviousEventNumber(0)
                        .withSource(source)
                        .withComponentName(componentName)
                        .build()
        );

        final ProcessedEvent latestProcessedEvent = processedEventTrackItem()
                .withEventNumber(1)
                .withPreviousEventNumber(0)
                .withSource(source)
                .withComponentName(componentName)
                .build();

        final StreamCloseSpy streamCloseSpy = new StreamCloseSpy();
        final Stream<ProcessedEvent> processedEventTrackItemStream = processedEvents.stream().onClose(streamCloseSpy);

        when(processedEventTrackingRepository.getLatestProcessedEvent(source, componentName)).thenReturn(of(latestProcessedEvent));
        when(processedEventStreamer.getProcessedEventStream(source, componentName, runFromEventNumber)).thenReturn(processedEventTrackItemStream);

        final List<MissingEventRange> missingEventRanges = missingEventRangeFinder.getRangesOfMissingEvents(
                source,
                componentName,
                runFromEventNumber,
                highestPublishedEventNumber);

        assertThat(missingEventRanges.size(), is(1));

        assertThat(missingEventRanges.get(0).getMissingEventFrom(), is(2L));
        assertThat(missingEventRanges.get(0).getMissingEventTo(), is(highestExclusiveEventNumber));

        assertThat(streamCloseSpy.streamClosed(), is(true));
    }

    @Test
    public void shouldHandleARangeOfMissingEventsOfJustOneMissingEvent() throws Exception {

        final String source = "example-context";
        final String componentName = "EVENT_LISTENER";
        final Long runFromEventNumber = 23L;
        final Long highestPublishedEventNumber = 23L;
        final Long highestExclusiveEventNumber = highestPublishedEventNumber + 1;

        final List<ProcessedEvent> processedEvents = asList(
                processedEventTrackItem()
                        .withEventNumber(3)
                        .withPreviousEventNumber(2)
                        .withSource(source)
                        .withComponentName(componentName)
                        .build(),
                processedEventTrackItem()
                        .withEventNumber(1)
                        .withPreviousEventNumber(0)
                        .withSource(source)
                        .withComponentName(componentName)
                        .build()
        );

        final ProcessedEvent latestProcessedEvent = processedEventTrackItem()
                .withEventNumber(3)
                .withPreviousEventNumber(2)
                .withSource(source)
                .withComponentName(componentName)
                .build();

        final StreamCloseSpy streamCloseSpy = new StreamCloseSpy();
        final Stream<ProcessedEvent> processedEventTrackItemStream = processedEvents.stream().onClose(streamCloseSpy);

        when(processedEventTrackingRepository.getLatestProcessedEvent(source, componentName)).thenReturn(of(latestProcessedEvent));
        when(processedEventStreamer.getProcessedEventStream(source, componentName, runFromEventNumber)).thenReturn(processedEventTrackItemStream);

        final List<MissingEventRange> missingEventRanges = missingEventRangeFinder.getRangesOfMissingEvents(
                source,
                componentName,
                runFromEventNumber,
                highestPublishedEventNumber);

        assertThat(missingEventRanges.size(), is(2));

        assertThat(missingEventRanges.get(0).getMissingEventFrom(), is(2L));
        assertThat(missingEventRanges.get(0).getMissingEventTo(), is(3L));
        assertThat(missingEventRanges.get(1).getMissingEventFrom(), is(4L));
        assertThat(missingEventRanges.get(1).getMissingEventTo(), is(highestExclusiveEventNumber));

        assertThat(streamCloseSpy.streamClosed(), is(true));
    }

    @Test
    public void shouldReturnMissingEventIfPreviousEventNumberIsNotZero() throws Exception {

        final String source = "example-context";
        final String componentName = "EVENT_LISTENER";
        final Long runFromEventNumber = 23L;
        final Long highestPublishedEventNumber = 23L;
        final Long highestExclusiveEventNumber = highestPublishedEventNumber + 1;

        final List<ProcessedEvent> processedEvents = singletonList(
                processedEventTrackItem()
                        .withEventNumber(2)
                        .withPreviousEventNumber(1)
                        .withSource(source)
                        .withComponentName(componentName)
                        .build()
        );

        final ProcessedEvent latestProcessedEvent = processedEventTrackItem()
                .withEventNumber(2)
                .withPreviousEventNumber(1)
                .withSource(source)
                .withComponentName(componentName)
                .build();

        final StreamCloseSpy streamCloseSpy = new StreamCloseSpy();
        final Stream<ProcessedEvent> processedEventTrackItemStream = processedEvents.stream().onClose(streamCloseSpy);

        when(processedEventTrackingRepository.getLatestProcessedEvent(source, componentName)).thenReturn(of(latestProcessedEvent));
        when(processedEventStreamer.getProcessedEventStream(source, componentName, runFromEventNumber)).thenReturn(processedEventTrackItemStream);

        final List<MissingEventRange> missingEventRanges = missingEventRangeFinder.getRangesOfMissingEvents(
                source,
                componentName,
                runFromEventNumber,
                highestPublishedEventNumber);

        assertThat(missingEventRanges.size(), is(2));

        assertThat(missingEventRanges.get(0).getMissingEventFrom(), is(1L));
        assertThat(missingEventRanges.get(0).getMissingEventTo(), is(2L));
        assertThat(missingEventRanges.get(1).getMissingEventFrom(), is(3L));
        assertThat(missingEventRanges.get(1).getMissingEventTo(), is(highestExclusiveEventNumber));

        assertThat(streamCloseSpy.streamClosed(), is(true));
    }
}