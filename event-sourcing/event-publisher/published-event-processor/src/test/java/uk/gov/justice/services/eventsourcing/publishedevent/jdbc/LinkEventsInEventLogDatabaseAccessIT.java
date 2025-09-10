package uk.gov.justice.services.eventsourcing.publishedevent.jdbc;

import static java.lang.Long.MIN_VALUE;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.fromSqlTimestamp;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;
import uk.gov.justice.services.test.utils.persistence.FrameworkTestDataSourceFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.ZonedDateTime;
import java.util.Optional;
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
    public void shouldGetPreviousEventNumberOfZeroIfNoEventsExistInEventLog() throws Exception {

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(eventStoreDataSource);

        assertThat(linkEventsInEventLogDatabaseAccess.findNextUnlinkedPreviousEventNumber(), is(0L));

    }

    @Test
    public void shouldFindTheNextUnlinkedPreviousEventNumber() throws Exception {

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(eventStoreDataSource);

        final Long expectedPreviousEventNumber = insertEventAndGetItsEventNumber();
        insertNewEventWithoutPreviousEventNumber(1);

        assertThat(linkEventsInEventLogDatabaseAccess.findNextUnlinkedPreviousEventNumber(), is(expectedPreviousEventNumber));
    }

    @Test
    public void shouldFindTheEarliestEventWithoutPreviousEventNumber() throws Exception {

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(eventStoreDataSource);

        // add an event with previous event number
        insertEventAndGetItsEventNumber();

        final UUID firstUnlinkedEventId = insertNewEventWithoutPreviousEventNumber(1);
        insertNewEventWithoutPreviousEventNumber(2);
        insertNewEventWithoutPreviousEventNumber(3);

        final Optional<LinkableEventDetails> nextUnlinkedEvent = linkEventsInEventLogDatabaseAccess.findNextUnlinkedEvent();

        assertThat(nextUnlinkedEvent.isPresent(), is(true));

        assertThat(nextUnlinkedEvent.get().eventId(), is(firstUnlinkedEventId));
        assertThat(nextUnlinkedEvent.get().eventNumber(), is(notNullValue()));
        assertThat(nextUnlinkedEvent.get().metadata(), is("some-event-metadata_1"));
    }

    @Test
    public void shouldAddPreviousEventNumberAndUpdatedMetadataToUnlinkedEvent() throws Exception {

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(eventStoreDataSource);

        final Long previousEventNumber = insertEventAndGetItsEventNumber();
        final UUID unlinkedEventId = insertNewEventWithoutPreviousEventNumber(1);
        final String updatedMetadata = "new-metadata";

        linkEventsInEventLogDatabaseAccess.linkEventInEventLogTable(unlinkedEventId, previousEventNumber, updatedMetadata);

        final String sql = """
            SELECT previous_event_number, metadata FROM event_log WHERE id = ?
        """;
        try (final Connection connection = eventStoreDataSource.getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            preparedStatement.setObject(1, unlinkedEventId);

            try(final ResultSet resultSet = preparedStatement.executeQuery()) {

                if (resultSet.next()) {
                    assertThat(resultSet.getLong(1), is(previousEventNumber));
                    assertThat(resultSet.getString(2), is(updatedMetadata));
                } else {
                    fail();
                }
            }
        }
    }

    @Test
    public void shouldAddEventToPublishQueue() throws Exception {

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

    private Long insertEventAndGetItsEventNumber() throws Exception {

        final UUID eventId = randomUUID();

        final String insertSql = """
                INSERT INTO event_log (
                       id,
                       stream_id,
                       position_in_stream,
                       name,
                       payload,
                       metadata,
                       previous_event_number)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """;
        try (final Connection connection = eventStoreDataSource.getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(insertSql)) {

            preparedStatement.setObject(1, eventId);
            preparedStatement.setObject(2, randomUUID());
            preparedStatement.setLong(3, 1L);
            preparedStatement.setString(4, "some-event-name-");
            preparedStatement.setString(5, "some-event-payload");
            preparedStatement.setString(6, "some-event-metadata");
            preparedStatement.setLong(7, 0L);

            preparedStatement.execute();

        }

        final String getItsEventNumberSql = """
                SELECT event_number FROM event_log WHERE id = ?
                """;

        try (final Connection connection = eventStoreDataSource.getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(getItsEventNumberSql)) {

            preparedStatement.setObject(1, eventId);

            try(final ResultSet resultSet = preparedStatement.executeQuery()) {

                if (resultSet.next()) {
                    return resultSet.getLong(1);
                } else {
                    fail();
                }
            }

        }

        // should never happen
        return MIN_VALUE;
    }

    private UUID insertNewEventWithoutPreviousEventNumber(final int insertionOrder) throws Exception {

        final UUID eventId = randomUUID();

        final String insertSql = """
                INSERT INTO event_log (
                       id,
                       stream_id,
                       position_in_stream,
                       name,
                       payload,
                       metadata)
                VALUES (?, ?, ?, ?, ?, ?)
                """;
        try (final Connection connection = eventStoreDataSource.getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(insertSql)) {

            preparedStatement.setObject(1, eventId);
            preparedStatement.setObject(2, randomUUID());
            preparedStatement.setLong(3, 4L);
            preparedStatement.setString(4, "some-event-name_" + insertionOrder);
            preparedStatement.setString(5, "some-event-payload_" + insertionOrder);
            preparedStatement.setString(6, "some-event-metadata_" + insertionOrder);

            preparedStatement.execute();

        }

        return eventId;
    }
}