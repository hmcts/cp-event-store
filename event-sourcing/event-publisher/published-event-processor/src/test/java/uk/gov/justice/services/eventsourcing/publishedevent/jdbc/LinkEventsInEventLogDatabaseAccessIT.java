package uk.gov.justice.services.eventsourcing.publishedevent.jdbc;

import static java.util.Optional.of;
import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.fromSqlTimestamp;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.toSqlTimestamp;

import java.sql.Statement;
import org.postgresql.util.PSQLException;
import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;
import uk.gov.justice.services.test.utils.persistence.FrameworkTestDataSourceFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.ZonedDateTime;
import java.util.UUID;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class LinkEventsInEventLogDatabaseAccessIT {

    private final DataSource eventStoreDataSource = new FrameworkTestDataSourceFactory().createEventStoreDataSource();

    @Mock
    private EventStoreDataSourceProvider eventStoreDataSourceProvider;

    @Mock
    private UtcClock clock;
    
    @InjectMocks
    private LinkEventsInEventLogDatabaseAccess linkEventsInEventLogDatabaseAccess;

    @BeforeEach
    public void cleanEventLogTables() {
        new DatabaseCleaner().cleanEventStoreTables("framework");
    }

    @Test
    public void shouldAddEventNumbersToEventInEventLogTable() throws Exception {

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(eventStoreDataSource);

        final UUID eventId = randomUUID();
        final Long eventNumber = 23L;
        final Long previousEventNumber = 22L;
        insertUnlinkedEventIntoEventLogTable(eventId, new UtcClock().now(), 1);

        linkEventsInEventLogDatabaseAccess.linkEvent(eventId, eventNumber, previousEventNumber);

        final String sql = """
            SELECT event_number, previous_event_number
            FROM event_log
            WHERE id = ?
        """;
        try(final Connection connection = eventStoreDataSource.getConnection();
            final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            preparedStatement.setObject(1, eventId);
            try(final ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    assertThat(resultSet.getLong("event_number"), is(eventNumber));
                    assertThat(resultSet.getLong("previous_event_number"), is(previousEventNumber));
                } else {
                    fail();
                }
            }
        }
    }

    @Test
    public void shouldSetStatementTimeoutOnCurrentTransaction() throws Exception {
        insertUnlinkedEventIntoEventLogTable(randomUUID(), new UtcClock().now(), 1);

        final DataSource spyDataSource = spy(new FrameworkTestDataSourceFactory().createEventStoreDataSource());
        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(spyDataSource);
        try(final Connection spyConnection = spy(spyDataSource.getConnection())) {
            spyConnection.setAutoCommit(false);
            when(spyDataSource.getConnection()).thenReturn(spyConnection);
            //This is required as production code closes the connection but same connection is used below in test, to run query for validating statement timeout behaviour
            doNothing().doCallRealMethod().when(spyConnection).close();

            //when
            linkEventsInEventLogDatabaseAccess.setStatementTimeoutOnCurrentTransaction(1);
            final PSQLException pgException = assertThrows(
                    PSQLException.class,
                    () -> {
                        try (final Statement statement = spyConnection.createStatement()) {
                            statement.executeQuery("SELECT pg_sleep(10) FROM event_log");
                        }
                    });

            assertThat(pgException.getMessage(), containsString("ERROR: canceling statement due to statement timeout"));
        }
    }

    @Test
    public void shouldGetTheHighestCurrentlyAllocatedEventNumberFromEventLogTable() throws Exception {

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(eventStoreDataSource);

        final long expectedHighestEventNumber = 4L;
        final ZonedDateTime oneMinuteAgo = new UtcClock().now().minusMinutes(1);

        insertLinkedEventIntoEventLogTable(randomUUID(), 1L, 0L, oneMinuteAgo.plusSeconds(1), 1);
        insertLinkedEventIntoEventLogTable(randomUUID(), 2L, 1L, oneMinuteAgo.plusSeconds(2), 2);
        insertLinkedEventIntoEventLogTable(randomUUID(), 3L, 2L, oneMinuteAgo.plusSeconds(3), 3);
        insertLinkedEventIntoEventLogTable(randomUUID(), expectedHighestEventNumber, 3L, oneMinuteAgo.plusSeconds(4), 4);
        insertUnlinkedEventIntoEventLogTable(randomUUID(), oneMinuteAgo.plusSeconds(5), 5);
        insertUnlinkedEventIntoEventLogTable(randomUUID(), oneMinuteAgo.plusSeconds(6), 6);
        insertUnlinkedEventIntoEventLogTable(randomUUID(), oneMinuteAgo.plusSeconds(7), 7);

        assertThat(linkEventsInEventLogDatabaseAccess.findCurrentHighestEventNumberInEventLogTable(), is(expectedHighestEventNumber));
    }

    @Test
    public void shouldReturnCurrentHighestEventNumberOfZeroIfNoEventsInEventLogTable() throws Exception {
        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(eventStoreDataSource);
        
        assertThat(linkEventsInEventLogDatabaseAccess.findCurrentHighestEventNumberInEventLogTable(), is(0L));
    }

    @Test
    public void shouldGetTheEventIdOfTheEventWithTheHighestEventNumber() throws Exception {

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(eventStoreDataSource);

        final UUID eventIdToFind = fromString("327a085e-6d23-4925-8a9a-4fe0c4400004");

        final ZonedDateTime oneMinuteAgo = new UtcClock().now().minusMinutes(1);

        insertLinkedEventIntoEventLogTable(fromString("327a085e-6d23-4925-8a9a-4fe0c4400001"), 1L, 0L, oneMinuteAgo.plusSeconds(1), 1);
        insertLinkedEventIntoEventLogTable(fromString("327a085e-6d23-4925-8a9a-4fe0c4400002"), 2L, 1L, oneMinuteAgo.plusSeconds(2), 2);
        insertLinkedEventIntoEventLogTable(fromString("327a085e-6d23-4925-8a9a-4fe0c4400003"), 3L, 2L, oneMinuteAgo.plusSeconds(3), 3);

        insertUnlinkedEventIntoEventLogTable(eventIdToFind, oneMinuteAgo.plusSeconds(4), 4);
        insertUnlinkedEventIntoEventLogTable(fromString("327a085e-6d23-4925-8a9a-4fe0c4400005"), oneMinuteAgo.plusSeconds(5), 5);
        insertUnlinkedEventIntoEventLogTable(fromString("327a085e-6d23-4925-8a9a-4fe0c4400006"), oneMinuteAgo.plusSeconds(6), 6);
        insertUnlinkedEventIntoEventLogTable(fromString("327a085e-6d23-4925-8a9a-4fe0c4400007"), oneMinuteAgo.plusSeconds(7), 7);

        assertThat(linkEventsInEventLogDatabaseAccess.findIdOfNextEventToLink(), is(of(eventIdToFind)));
    }

    @Test
    public void shouldAddEventToPublishQueue() throws Exception {

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(eventStoreDataSource);
        
        final ZonedDateTime now = new UtcClock().now();
        final UUID eventId = randomUUID();

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(eventStoreDataSource);
        when(clock.now()).thenReturn(now);

        linkEventsInEventLogDatabaseAccess.insertLinkedEventIntoPublishQueue(eventId);

        final String sql = """
            SELECT date_queued
            FROM publish_queue
            WHERE event_log_id = ?
        """;
        try(final Connection connection = eventStoreDataSource.getConnection();
            final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            preparedStatement.setObject(1, eventId);
            try(final ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    assertThat(fromSqlTimestamp(resultSet.getTimestamp(1)), is(now));
                } else {
                    fail();
                }
            }
       }
    }

    private void insertUnlinkedEventIntoEventLogTable(
            final UUID eventId,
            final ZonedDateTime dateCreated,
            final int insertionOrder) throws Exception {

        final String insertSql = """
                INSERT INTO event_log (
                       id,
                       stream_id,
                       position_in_stream,
                       name,
                       date_created,
                       payload,
                       metadata,
                       event_number)
                VALUES (?, ?, ?, ?, ?, ?, ?, null)
                """;

        try (final Connection connection = eventStoreDataSource.getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(insertSql)) {

            preparedStatement.setObject(1, eventId);
            preparedStatement.setObject(2, randomUUID());
            preparedStatement.setLong(3, 1L);
            preparedStatement.setString(4, "some-event-name-" + insertionOrder);
            preparedStatement.setTimestamp(5, toSqlTimestamp(dateCreated));
            preparedStatement.setString(6, "some-event-payload" + insertionOrder);
            preparedStatement.setString(7, "some-event-metadata" + insertionOrder);

            preparedStatement.execute();
        }
    }

    private void insertLinkedEventIntoEventLogTable(
            final UUID eventId,
            final Long eventNumber,
            final Long previousEventNumber,
            final ZonedDateTime dateCreated,
            final int insertionOrder) throws Exception {

        final String insertSql = """
                INSERT INTO event_log (
                       id,
                       stream_id,
                       position_in_stream,
                       name,
                       date_created,
                       payload,
                       metadata,
                       event_number,
                       previous_event_number)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;

        try (final Connection connection = eventStoreDataSource.getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(insertSql)) {

            preparedStatement.setObject(1, eventId);
            preparedStatement.setObject(2, randomUUID());
            preparedStatement.setLong(3, insertionOrder);
            preparedStatement.setString(4, "some-event-name_" + insertionOrder);
            preparedStatement.setTimestamp(5, toSqlTimestamp(dateCreated));
            preparedStatement.setString(6, "some-event-payload_" + insertionOrder);
            preparedStatement.setString(7, "some-event-metadata_" + insertionOrder);
            preparedStatement.setLong(8, eventNumber);
            preparedStatement.setLong(9, previousEventNumber);

            preparedStatement.execute();
        }
    }
}