package uk.gov.justice.services.eventsourcing.publishedevent.jdbc;

import static java.lang.String.format;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.toSqlTimestamp;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;

import javax.inject.Inject;

public class LinkEventsInEventLogDatabaseAccess {

    private static final String GET_LATEST_SEQUENCED_QUERY = """
            SELECT event_number FROM event_log
            WHERE previous_event_number IS NOT NULL
            ORDER BY event_number DESC LIMIT 1 FOR UPDATE
            """;

    private static final String GET_EARLIEST_UNSEQUENCED_QUERY = """
            SELECT id, event_number, metadata FROM event_log
            WHERE previous_event_number is NULL
            ORDER BY event_number LIMIT 1 FOR UPDATE SKIP LOCKED
            """;

    private static final String LINK_EVENTS_QUERY = """
            UPDATE event_log SET
            previous_event_number = ?,
            metadata = ?
            WHERE id = ?
            """;

    private static final String INSERT_EVENT_INTO_PUBLISH_QUEUE_QUERY = """
            INSERT INTO publish_queue (event_log_id, date_queued) VALUES (?, ?)
            """;

    private static final Long DEFAULT_FIRST_PREVIOUS_EVENT_NUMBER = 0L;

    @Inject
    private EventStoreDataSourceProvider eventStoreDataSourceProvider;

    @Inject
    private UtcClock clock;

    public Long findNextUnlinkedPreviousEventNumber() {

        try (final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(GET_LATEST_SEQUENCED_QUERY);
             final ResultSet resultSet = preparedStatement.executeQuery()) {

            if (resultSet.next()) {
                return resultSet.getLong("event_number");
            }

            return DEFAULT_FIRST_PREVIOUS_EVENT_NUMBER;

        } catch (final SQLException e) {
            throw new EventNumberLinkingException("Failed to find next unlinked previousEventNumber in event_log table", e);
        }
    }

    public Optional<LinkableEventDetails> findNextUnlinkedEvent() {

        try (final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(GET_EARLIEST_UNSEQUENCED_QUERY);
             final ResultSet resultSet = preparedStatement.executeQuery()) {

            if (resultSet.next()) {
                final UUID eventId = resultSet.getObject("id", UUID.class);
                final Long eventNumber = resultSet.getLong("event_number");
                final String metadata = resultSet.getString("metadata");
                final LinkableEventDetails linkableEventDetails = new LinkableEventDetails(
                        eventId,
                        eventNumber,
                        metadata);

                return of(linkableEventDetails);
            }

            return empty();

        } catch (final SQLException e) {
            throw new EventNumberLinkingException("Failed to find next unlinked event in event_log table.", e);
        }
    }

    public void linkEventInEventLogTable(
            final UUID eventId,
            final Long previousEventNumber,
            final String metadata) {

        try (final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(LINK_EVENTS_QUERY)) {

            preparedStatement.setLong(1, previousEventNumber);
            preparedStatement.setString(2, metadata);
            preparedStatement.setObject(3, eventId);

            preparedStatement.executeUpdate();

        } catch (final SQLException e) {
            throw new EventNumberLinkingException(format("Failed to link event in event_log table. eventId: '%s', previousEventNumber: %d", eventId, previousEventNumber), e);
        }
    }

    public void insertLinkedEventIntoPublishQueue(final UUID eventId) {

        try (final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(INSERT_EVENT_INTO_PUBLISH_QUEUE_QUERY)) {

            preparedStatement.setObject(1, eventId);
            preparedStatement.setTimestamp(2, toSqlTimestamp(clock.now()));
            preparedStatement.executeUpdate();

        } catch (final SQLException e) {
            throw new EventNumberLinkingException(format("Failed to insert linked event into publish_queue table. eventId: '%s'", eventId), e);
        }
    }
}
