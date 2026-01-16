package uk.gov.justice.services.eventsourcing.eventpoller;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.Event;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventJdbcRepository;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EventStreamPollerBeanTest {

    @Mock
    private EventJdbcRepository eventRepository;

    @Mock
    private EventStreamPollerConfig eventStreamPollerConfig;

    @InjectMocks
    private EventStreamPollerBean eventStreamPollerBean;

    @Test
    public void shouldPollStreamEventsWithCorrectParameters() {
        final UUID streamId = UUID.randomUUID();
        final long fromPosition = 1L;
        final long toPosition = 5L;
        final int batchSize = 10;

        final Event event1 = createEvent(streamId, 1L);
        final Event event2 = createEvent(streamId, 2L);
        final Event event3 = createEvent(streamId, 3L);
        final Stream<Event> expectedStream = Stream.of(event1, event2, event3);

        when(eventStreamPollerConfig.getBatchSize()).thenReturn(batchSize);
        when(eventRepository.findByStreamIdInPositionRangeOrderByPositionAsc(streamId, fromPosition, toPosition, batchSize))
                .thenReturn(expectedStream);

        final Stream<Event> result = eventStreamPollerBean.pollStreamEvents(streamId, fromPosition, toPosition);


        verify(eventStreamPollerConfig).getBatchSize();
        verify(eventRepository).findByStreamIdInPositionRangeOrderByPositionAsc(streamId, fromPosition, toPosition, batchSize);

        final List<Event> resultList = result.toList();
        assertThat(resultList.size(), is(3));
        assertThat(resultList.get(0), is(event1));
        assertThat(resultList.get(1), is(event2));
        assertThat(resultList.get(2), is(event3));

    }


    @Test
    public void shouldHandleEmptyStream() {

        final UUID streamId = UUID.randomUUID();
        final long fromPosition = 5L;
        final long toPosition = 10L;
        final int batchSize = 25;

        final Stream<Event> emptyStream = Stream.empty();

        when(eventStreamPollerConfig.getBatchSize()).thenReturn(batchSize);
        when(eventRepository.findByStreamIdInPositionRangeOrderByPositionAsc(streamId, fromPosition, toPosition, batchSize))
                .thenReturn(emptyStream);

        final Stream<Event> result = eventStreamPollerBean.pollStreamEvents(streamId, fromPosition, toPosition);

        final List<Event> resultList = result.toList();
        assertThat(resultList.isEmpty(), is(true));
    }


    private Event createEvent(final UUID streamId, final Long positionInStream) {
        return new Event(
                UUID.randomUUID(),
                streamId,
                positionInStream,
                "test.event",
                "{}",
                "{}",
                ZonedDateTime.now()
        );
    }
}

