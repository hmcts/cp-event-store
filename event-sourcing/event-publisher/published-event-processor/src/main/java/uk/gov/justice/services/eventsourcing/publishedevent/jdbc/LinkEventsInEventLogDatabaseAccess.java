package uk.gov.justice.services.eventsourcing.publishedevent.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.inject.Inject;
import org.slf4j.Logger;
import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.publishedevent.EventPublishingException;
import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;

import static uk.gov.justice.services.common.converter.ZonedDateTimes.toSqlTimestamp;

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

    static final String SELECT_BATCH_OF_UNLINKED_EVENT_IDS = """
                SELECT id
                FROM event_log
                WHERE event_number IS NULL
                ORDER BY date_created
                LIMIT ?
                """;

    static final String INSERT_EVENT_INTO_PUBLISH_QUEUE_QUERY = """
            INSERT INTO publish_queue (event_log_id, date_queued) VALUES (?, ?)
            """;

    private static final Long DEFAULT_FIRST_PREVIOUS_EVENT_NUMBER = 0L;

    @Inject
    private EventStoreDataSourceProvider eventStoreDataSourceProvider;

    @Inject
    private Logger logger;

    @Inject
    private UtcClock clock;

    public Connection getEventStoreConnection() {
        try {
            return eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
        } catch (final SQLException e) {
            throw new EventPublishingException("Failed to get event store connection", e);
        }
    }

    public void setStatementTimeoutOnCurrentTransaction(final Connection connection, final int statementTimeoutInSecs) {
        try (final Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("SET LOCAL statement_timeout = '" + statementTimeoutInSecs + "s'");
        } catch (final SQLException e) {
            logger.info("Failed to set local statement timeout (safe to ignore)", e);
        }
    }

    public Long findCurrentHighestEventNumberInEventLogTable(final Connection connection) {
        try (final PreparedStatement preparedStatement = connection.prepareStatement(SELECT_HIGHEST_LINKED_EVENT_NUMBER_SQL);
             final ResultSet resultSet = preparedStatement.executeQuery()) {

            if (resultSet.next()) {
                return resultSet.getLong(1);
            }

            return DEFAULT_FIRST_PREVIOUS_EVENT_NUMBER;

        } catch (final SQLException e) {
            throw new EventPublishingException("Failed to find highest event number in event log table", e);
        }
    }

    public List<UUID> findBatchOfNextEventIdsToLink(final Connection connection, final int batchSize) {
        try (final PreparedStatement preparedStatement = connection.prepareStatement(SELECT_BATCH_OF_UNLINKED_EVENT_IDS)) {
            preparedStatement.setInt(1, batchSize);

            try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                final List<UUID> eventIds = new ArrayList<>(batchSize);
                while (resultSet.next()) {
                    eventIds.add(resultSet.getObject(1, UUID.class));
                }
                return eventIds;
            }
        } catch (final SQLException e) {
            throw new EventPublishingException("Failed to find batch of event ids to link", e);
        }
    }

    public void linkEventsBatch(final Connection connection, final List<LinkedEventData> linkDataList) {
        try (final PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_EVENT_NUMBERS_FOR_EVENT)) {
            for (final LinkedEventData linkData : linkDataList) {
                preparedStatement.setLong(1, linkData.eventNumber());
                preparedStatement.setLong(2, linkData.previousEventNumber());
                preparedStatement.setObject(3, linkData.eventId());
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
        } catch (final SQLException e) {
            throw new EventPublishingException("Failed to batch link events in event_log table", e);
        }
    }

    public void insertBatchIntoPublishQueue(final Connection connection, final List<UUID> eventIds) {
        try (final PreparedStatement preparedStatement = connection.prepareStatement(INSERT_EVENT_INTO_PUBLISH_QUEUE_QUERY)) {
            for (final UUID eventId : eventIds) {
                preparedStatement.setObject(1, eventId);
                preparedStatement.setTimestamp(2, toSqlTimestamp(clock.now()));
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
        } catch (final SQLException e) {
            throw new EventPublishingException("Failed to batch insert events into publish_queue table", e);
        }
    }
}