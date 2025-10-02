package uk.gov.justice.services.eventsourcing.publishedevent.jdbc;

import static java.lang.String.format;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.toSqlTimestamp;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.publishedevent.EventPublishingException;
import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;

import javax.inject.Inject;

public class LinkEventsInEventLogDatabaseAccess {

    static final String UPDATE_EVENT_NUMBERS_FOR_EVENT = """
            UPDATE event_log
            SET
                event_number = ?,
                previous_event_number = ?
            WHERE id = ?
            """;

    static final String SELECT_HIGHEST_LINKED_EVENT_NUMBER_SQL = """
                SELECT MAX(event_number)
                FROM event_log
                """;

    static final String SELECT_NEXT_UNLINKED_EVENT_ID = """
                SELECT id
                FROM event_log
                WHERE event_number IS NULL
                ORDER BY date_created
                LIMIT 1
                """;

    static final String INSERT_EVENT_INTO_PUBLISH_QUEUE_QUERY = """
            INSERT INTO publish_queue (event_log_id, date_queued) VALUES (?, ?)
            """;

    private static final Long DEFAULT_FIRST_PREVIOUS_EVENT_NUMBER = 0L;

    @Inject
    private EventStoreDataSourceProvider eventStoreDataSourceProvider;

    @Inject
    private UtcClock clock;

    public void linkEvent(final UUID eventId, final Long eventNumber, final Long previousEventNumber) {

        try (final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_EVENT_NUMBERS_FOR_EVENT)) {
            preparedStatement.setLong(1, eventNumber);
            preparedStatement.setLong(2, previousEventNumber);
            preparedStatement.setObject(3, eventId);
            preparedStatement.executeUpdate();

        } catch (final SQLException e) {
            throw new EventPublishingException(
                    format("Failed to link event in event_log table. eventId '%s' eventNumber %d, previousEventNumber %d",
                            eventId,
                            eventNumber,
                            previousEventNumber),
                    e);
        }
    }

    public Long findCurrentHighestEventNumberInEventLogTable() {

        try (final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(SELECT_HIGHEST_LINKED_EVENT_NUMBER_SQL);
             final ResultSet resultSet = preparedStatement.executeQuery()) {

            if (resultSet.next()) {
                return resultSet.getLong(1);
            }

            return DEFAULT_FIRST_PREVIOUS_EVENT_NUMBER;

        } catch (final SQLException e) {
            throw new EventPublishingException("Failed to find highest event number in event log table", e);
        }
    }

    public Optional<UUID> findIdOfNextEventToLink() {

        try (final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(SELECT_NEXT_UNLINKED_EVENT_ID);
             final ResultSet resultSet = preparedStatement.executeQuery()) {

            if (resultSet.next()) {
                return of(resultSet.getObject(1, UUID.class));
            }

            return empty();
        } catch (final SQLException e) {
            throw new EventPublishingException("Failed find event id to link", e);
        }
    }

    public void insertLinkedEventIntoPublishQueue(final UUID eventId) {

        try (final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(INSERT_EVENT_INTO_PUBLISH_QUEUE_QUERY)) {

            preparedStatement.setObject(1, eventId);
            preparedStatement.setTimestamp(2, toSqlTimestamp(clock.now()));
            preparedStatement.executeUpdate();

        } catch (final SQLException e) {
            throw new EventPublishingException(format("Failed to insert linked event into publish_queue table. eventId: '%s'", eventId), e);
        }
    }
}
