package uk.gov.justice.services.eventsourcing.publishedevent.jdbc;

import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.toSqlTimestamp;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;
import uk.gov.justice.services.test.utils.persistence.FrameworkTestDataSourceFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.ZonedDateTime;
import java.util.List;
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
    public void shouldGetEventStoreConnection() throws Exception {

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(eventStoreDataSource);

        try (final Connection connection = linkEventsInEventLogDatabaseAccess.getEventStoreConnection()) {
            assertThat(connection.isClosed(), is(false));
        }
    }

    @Test
    public void shouldFindCurrentHighestEventNumberUsingProvidedConnection() throws Exception {

        final ZonedDateTime oneMinuteAgo = new UtcClock().now().minusMinutes(1);
        insertLinkedEventIntoEventLogTable(randomUUID(), 1L, 0L, oneMinuteAgo.plusSeconds(1), 1);
        insertLinkedEventIntoEventLogTable(randomUUID(), 2L, 1L, oneMinuteAgo.plusSeconds(2), 2);

        try (final Connection connection = eventStoreDataSource.getConnection()) {
            assertThat(linkEventsInEventLogDatabaseAccess.findCurrentHighestEventNumberInEventLogTable(connection), is(2L));
        }
    }

    @Test
    public void shouldFindBatchOfNextEventIdsToLink() throws Exception {

        final UUID eventId1 = fromString("327a085e-6d23-4925-8a9a-4fe0c4400001");
        final UUID eventId2 = fromString("327a085e-6d23-4925-8a9a-4fe0c4400002");
        final UUID eventId3 = fromString("327a085e-6d23-4925-8a9a-4fe0c4400003");

        final ZonedDateTime oneMinuteAgo = new UtcClock().now().minusMinutes(1);
        insertLinkedEventIntoEventLogTable(randomUUID(), 1L, 0L, oneMinuteAgo.plusSeconds(1), 1);
        insertUnlinkedEventIntoEventLogTable(eventId1, oneMinuteAgo.plusSeconds(2), 2);
        insertUnlinkedEventIntoEventLogTable(eventId2, oneMinuteAgo.plusSeconds(3), 3);
        insertUnlinkedEventIntoEventLogTable(eventId3, oneMinuteAgo.plusSeconds(4), 4);

        try (final Connection connection = eventStoreDataSource.getConnection()) {
            final List<UUID> result = linkEventsInEventLogDatabaseAccess.findBatchOfNextEventIdsToLink(connection, 2);
            assertThat(result.size(), is(2));
            assertThat(result.get(0), is(eventId1));
            assertThat(result.get(1), is(eventId2));
        }
    }

    @Test
    public void shouldLinkEventsBatchAndInsertBatchIntoPublishQueue() throws Exception {

        final UUID eventId1 = randomUUID();
        final UUID eventId2 = randomUUID();

        final ZonedDateTime now = new UtcClock().now();
        insertUnlinkedEventIntoEventLogTable(eventId1, now.minusSeconds(2), 1);
        insertUnlinkedEventIntoEventLogTable(eventId2, now.minusSeconds(1), 2);

        when(clock.now()).thenReturn(now);

        final List<LinkedEventData> linkDataList = List.of(
                new LinkedEventData(eventId1, 1L, 0L),
                new LinkedEventData(eventId2, 2L, 1L)
        );

        try (final Connection connection = eventStoreDataSource.getConnection()) {
            linkEventsInEventLogDatabaseAccess.linkEventsBatch(connection, linkDataList);
            linkEventsInEventLogDatabaseAccess.insertBatchIntoPublishQueue(connection, List.of(eventId1, eventId2));
        }

        // Verify linking
        final String verifySql = "SELECT event_number, previous_event_number FROM event_log WHERE id = ?";
        try (final Connection connection = eventStoreDataSource.getConnection();
             final PreparedStatement ps = connection.prepareStatement(verifySql)) {
            ps.setObject(1, eventId1);
            try (final ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next(), is(true));
                assertThat(rs.getLong("event_number"), is(1L));
                assertThat(rs.getLong("previous_event_number"), is(0L));
            }
        }

        // Verify publish queue
        final String queueSql = "SELECT count(*) FROM publish_queue WHERE event_log_id IN (?, ?)";
        try (final Connection connection = eventStoreDataSource.getConnection();
             final PreparedStatement ps = connection.prepareStatement(queueSql)) {
            ps.setObject(1, eventId1);
            ps.setObject(2, eventId2);
            try (final ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next(), is(true));
                assertThat(rs.getInt(1), is(2));
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