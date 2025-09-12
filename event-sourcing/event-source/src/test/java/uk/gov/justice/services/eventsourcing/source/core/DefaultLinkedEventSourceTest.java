package uk.gov.justice.services.eventsourcing.source.core;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.MultipleDataSourcePublishedEventRepository;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventsourcing.source.api.streams.MissingEventRange;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DefaultLinkedEventSourceTest {

    @Mock
    private MultipleDataSourcePublishedEventRepository multipleDataSourcePublishedEventRepository;

    @InjectMocks
    private DefaultPublishedEventSource defaultPublishedEventSource;

    @Test
    public void shouldFindEventsByEventNumber() throws Exception {

        final long eventNumber = 972834L;

        final LinkedEvent linkedEvent = mock(LinkedEvent.class);

        when(multipleDataSourcePublishedEventRepository.findEventsSince(eventNumber)).thenReturn(Stream.of(linkedEvent));

        final List<LinkedEvent> envelopes = defaultPublishedEventSource.findEventsSince(eventNumber).collect(toList());

        assertThat(envelopes.size(), is(1));
        assertThat(envelopes.get(0), is(linkedEvent));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldFindEventRange() throws Exception {

        final long fromEventNumber = 1L;
        final long toEventNumber = 10L;
        final MissingEventRange missingEventRange = new MissingEventRange(fromEventNumber, toEventNumber);
        final Stream streamOfEvents = mock(Stream.class);

        when(multipleDataSourcePublishedEventRepository.findEventRange(fromEventNumber, toEventNumber)).thenReturn(streamOfEvents);

        final Stream<LinkedEvent> eventRange = defaultPublishedEventSource.findEventRange(missingEventRange);

        assertThat(eventRange, is(streamOfEvents));
    }

    @Test
    public void findByEventIdShouldReturnEvent() throws Exception {

        final UUID eventId = randomUUID();
        final Optional<LinkedEvent> publishedEvent = of(mock(LinkedEvent.class));
        when(multipleDataSourcePublishedEventRepository.findByEventId(eventId)).thenReturn(publishedEvent);

        final Optional<LinkedEvent> fetchedEvent = defaultPublishedEventSource.findByEventId(eventId);

        assertThat(fetchedEvent, is(publishedEvent));
    }

    @Test
    public void shouldGetEventNumberFromLatestPublishedEvent() throws Exception {

        final Long latestEventNumber = 9827394873L;
        final LinkedEvent latestLinkedEvent = mock(LinkedEvent.class);
        when(latestLinkedEvent.getEventNumber()).thenReturn(of(latestEventNumber));
        when(multipleDataSourcePublishedEventRepository.getLatestPublishedEvent()).thenReturn(of(latestLinkedEvent));

        assertThat(defaultPublishedEventSource.getHighestPublishedEventNumber(), is(latestEventNumber));
    }

    @Test
    public void shouldReturnZeroIfLatestPublishedEventHasNoEventNumber() throws Exception {

        final LinkedEvent latestLinkedEvent = mock(LinkedEvent.class);
        when(latestLinkedEvent.getEventNumber()).thenReturn(empty());
        when(multipleDataSourcePublishedEventRepository.getLatestPublishedEvent()).thenReturn(of(latestLinkedEvent));

        assertThat(defaultPublishedEventSource.getHighestPublishedEventNumber(), is(0L));
    }

    @Test
    public void shouldReturnZeroIfNoLatestPublishedEventFound() throws Exception {

        when(multipleDataSourcePublishedEventRepository.getLatestPublishedEvent()).thenReturn(empty());

        assertThat(defaultPublishedEventSource.getHighestPublishedEventNumber(), is(0L));
    }
}
