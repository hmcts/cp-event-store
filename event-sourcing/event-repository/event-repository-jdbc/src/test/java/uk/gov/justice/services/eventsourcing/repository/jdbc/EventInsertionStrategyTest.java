package uk.gov.justice.services.eventsourcing.repository.jdbc;

import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.toSqlTimestamp;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.Event;
import uk.gov.justice.services.eventsourcing.repository.jdbc.exception.InvalidPositionException;
import uk.gov.justice.services.eventsourcing.repository.jdbc.exception.OptimisticLockingRetryException;
import uk.gov.justice.services.jdbc.persistence.PreparedStatementWrapper;

import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EventInsertionStrategyTest {

    @InjectMocks
    private EventInsertionStrategy eventInsertionStrategy;

    @Test
    public void shouldUseTheCorrectInsertStatement() throws Exception {

        final String expectedSql = """
            INSERT INTO
            event_log (
                 id,
                 stream_id,
                 position_in_stream,
                 name,
                 metadata,
                 payload,
                 date_created,
                 event_number,
                 previous_event_number,
                 is_published)
            VALUES(?, ?, ?, ?, ?, ?, ?, NULL, NULL, FALSE)
            ON CONFLICT DO NOTHING
            """;

        assertThat(eventInsertionStrategy.insertStatement(), is(expectedSql));
    }

    @Test
    public void shouldInsertCorrectValuesIntoPreparedStatementAndCallUpdate() throws Exception {

        final UUID eventId = randomUUID();
        final UUID streamId = randomUUID();
        final long positionInStream = 23L;
        final int rowsUpdatedCount = 1;

        final String name = "some-event-name";
        final String metadata = "some-metadata-json";
        final String payload = "some-payload-json";
        final ZonedDateTime createdAt = new UtcClock().now();
        final Timestamp createdAtTimestamp = toSqlTimestamp(createdAt);

        final Event event = new Event(
                eventId,
                streamId,
                positionInStream,
                name,
                metadata,
                payload,
                createdAt
        );

        final PreparedStatementWrapper preparedStatementWrapper = mock(PreparedStatementWrapper.class);

        when(preparedStatementWrapper.executeUpdate()).thenReturn(rowsUpdatedCount);

        eventInsertionStrategy.insert(preparedStatementWrapper, event);

        verify(preparedStatementWrapper).setObject(1, eventId);
        verify(preparedStatementWrapper).setObject(2, streamId);
        verify(preparedStatementWrapper).setLong(3, positionInStream);
        verify(preparedStatementWrapper).setString(4, name);
        verify(preparedStatementWrapper).setString(5, metadata);
        verify(preparedStatementWrapper).setString(6, payload);
        verify(preparedStatementWrapper).setTimestamp(7, createdAtTimestamp);

        verify(preparedStatementWrapper, never()).close();
    }

    @Test
    public void shouldThrowInvalidPositionExceptionIfTheEventHasNoPositionInStream() throws Exception {

        final UUID eventId = fromString("36602228-af06-4c13-b2eb-a51d24cd5a8c");
        final UUID streamId = fromString("55e1b067-666c-4b24-b38a-a1d665db2bde");

        final Event event = mock(Event.class);
        final PreparedStatementWrapper preparedStatementWrapper = mock(PreparedStatementWrapper.class);

        when(event.getPositionInStream()).thenReturn(null);
        when(event.getId()).thenReturn(eventId);
        when(event.getStreamId()).thenReturn(streamId);

        final InvalidPositionException invalidPositionException = assertThrows(
                InvalidPositionException.class,
                () -> eventInsertionStrategy.insert(preparedStatementWrapper, event));

        assertThat(invalidPositionException.getMessage(), is("Failed to insert event into event log table. Event has NULL positionInStream: event id '36602228-af06-4c13-b2eb-a51d24cd5a8c', streamId '55e1b067-666c-4b24-b38a-a1d665db2bde'"));
    }

    @Test
    public void shouldThrowOptimisticLockingRetryExceptionIfDatabaseConflictResultsInNoRowsGettingUpdated() throws Exception {

        final UUID eventId = randomUUID();
        final UUID streamId = fromString("fefc7af0-a93f-4019-983e-cec3fc7a816a");
        final long positionInStream = 23L;

        final String name = "some-event-name";
        final String metadata = "some-metadata-json";
        final String payload = "some-payload-json";
        final ZonedDateTime createdAt = new UtcClock().now();
        final Timestamp createdAtTimestamp = toSqlTimestamp(createdAt);

        final Event event = new Event(
                eventId,
                streamId,
                positionInStream,
                name,
                metadata,
                payload,
                createdAt
        );

        final PreparedStatementWrapper preparedStatementWrapper = mock(PreparedStatementWrapper.class);

        when(preparedStatementWrapper.executeUpdate()).thenReturn(0);

        final OptimisticLockingRetryException optimisticLockingRetryException = assertThrows(
                OptimisticLockingRetryException.class,
                () -> eventInsertionStrategy.insert(preparedStatementWrapper, event));

        assertThat(optimisticLockingRetryException.getMessage(), is("OptimisticLockingRetryException while storing positionInStream '23' of stream 'fefc7af0-a93f-4019-983e-cec3fc7a816a'"));

        verify(preparedStatementWrapper).setObject(1, eventId);
        verify(preparedStatementWrapper).setObject(2, streamId);
        verify(preparedStatementWrapper).setLong(3, positionInStream);
        verify(preparedStatementWrapper).setString(4, name);
        verify(preparedStatementWrapper).setString(5, metadata);
        verify(preparedStatementWrapper).setString(6, payload);
        verify(preparedStatementWrapper).setTimestamp(7, createdAtTimestamp);

        verify(preparedStatementWrapper, never()).close();

    }
}
