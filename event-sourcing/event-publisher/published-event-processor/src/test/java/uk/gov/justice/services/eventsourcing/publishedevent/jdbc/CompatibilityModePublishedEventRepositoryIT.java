package uk.gov.justice.services.eventsourcing.publishedevent.jdbc;

import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.toSqlTimestamp;
import static uk.gov.justice.services.messaging.spi.DefaultJsonMetadata.metadataBuilder;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.messaging.Metadata;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;
import uk.gov.justice.services.test.utils.persistence.FrameworkTestDataSourceFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CompatibilityModePublishedEventRepositoryIT {

    private final DataSource eventStoreDataSource = new FrameworkTestDataSourceFactory().createEventStoreDataSource();

    private final CompatibilityModePublishedEventRepository compatibilityModePublishedEventRepository = new CompatibilityModePublishedEventRepository();

    @BeforeEach
    public void cleanEventLogTables() {
        new DatabaseCleaner().cleanEventStoreTables("framework");
    }

    @Test
    public void shouldInsertBatchIntoPublishedEventTable() throws Exception {

        final UUID eventId1 = randomUUID();
        final UUID eventId2 = randomUUID();
        final ZonedDateTime createdAt = new UtcClock().now();

        insertEventIntoEventLog(eventId1, randomUUID(), 1L, "event-1", 1L, 0L, createdAt);
        insertEventIntoEventLog(eventId2, randomUUID(), 1L, "event-2", 2L, 1L, createdAt.plusSeconds(1));

        final List<LinkedEventData> linkDataList = List.of(
                new LinkedEventData(eventId1, 1L, 0L),
                new LinkedEventData(eventId2, 2L, 1L)
        );

        try (final Connection connection = eventStoreDataSource.getConnection()) {
            compatibilityModePublishedEventRepository.insertBatchIntoPublishedEvent(connection, linkDataList);
        }

        assertThat(countPublishedEvents(), is(2));
    }

    @Test
    public void shouldSetEventNumberSequenceUsingProvidedConnection() throws Exception {

        final Long currentSequenceNumber = getCurrentSequenceNumber();
        final Long newSequenceNumber = currentSequenceNumber + 42;

        try (final Connection connection = eventStoreDataSource.getConnection()) {
            compatibilityModePublishedEventRepository.setEventNumberSequenceTo(connection, newSequenceNumber);
        }

        assertThat(getCurrentSequenceNumber(), is(newSequenceNumber));
    }

    private int countPublishedEvents() throws SQLException {
        try (final Connection connection = eventStoreDataSource.getConnection();
             final PreparedStatement ps = connection.prepareStatement("SELECT COUNT(*) FROM published_event");
             final ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getInt(1) : 0;
        }
    }

    private Long getCurrentSequenceNumber() throws SQLException {
        try (final Connection connection = eventStoreDataSource.getConnection();
             final PreparedStatement ps = connection.prepareStatement("SELECT last_value FROM event_sequence_seq");
             final ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getLong(1);
            }
            throw new RuntimeException("Failed to get last value from 'event_sequence_seq' sequence");
        }
    }

    private void insertEventIntoEventLog(final UUID eventId, final UUID streamId, final long position,
                                          final String name, final Long eventNumber, final Long previousEventNumber,
                                          final ZonedDateTime createdAt) throws Exception {
        final Metadata metadata = metadataBuilder()
                .withId(eventId)
                .withName(name)
                .withStreamId(streamId)
                .createdAt(createdAt)
                .withPosition(position)
                .build();

        final String sql = """
                INSERT INTO event_log (id, stream_id, position_in_stream, name, payload, metadata, date_created, event_number, previous_event_number)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;

        try (final Connection connection = eventStoreDataSource.getConnection();
             final PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setObject(1, eventId);
            ps.setObject(2, streamId);
            ps.setLong(3, position);
            ps.setString(4, name);
            ps.setString(5, "{}");
            ps.setString(6, metadata.asJsonObject().toString());
            ps.setTimestamp(7, toSqlTimestamp(createdAt));
            ps.setLong(8, eventNumber);
            ps.setLong(9, previousEventNumber);
            ps.execute();
        }
    }
}