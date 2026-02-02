package uk.gov.justice.services.eventsourcing.repository.jdbc;

import static java.lang.String.format;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.fromString;
import static javax.transaction.Transactional.TxType.MANDATORY;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.toSqlTimestamp;

import uk.gov.justice.services.eventsourcing.repository.jdbc.PublishQueue.PublishQueueTableName;
import uk.gov.justice.services.eventsourcing.repository.jdbc.exception.PublishedEventException;
import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;

import javax.inject.Inject;
import javax.transaction.Transactional;

/**
 * The EventDeQueuer class provides a method that returns an event from the event_log.
 */
public class PublishQueuesDataAccess {

    private static final String INSERT_INTO_PUBLISH_TABLE_QUERY_PATTERN = "INSERT into %s (event_log_id, date_queued) values (?, ?)";
    private static final String SELECT_FROM_PUBLISH_TABLE_QUERY_PATTERN = "SELECT event_log_id FROM %s ORDER BY date_queued LIMIT 1 FOR UPDATE SKIP LOCKED ";
    private static final String DELETE_FROM_PUBLISH_TABLE_QUERY_PATTERN = "DELETE FROM %s where event_log_id = ?";
    private static final String COUNT_ROWS_QUERY_PATTERN = "SELECT COUNT (*) FROM %s";

    @Inject
    private EventStoreDataSourceProvider eventStoreDataSourceProvider;

    @Transactional(MANDATORY)
    public void addToQueue(final UUID eventId, final ZonedDateTime queuedAt, final PublishQueueTableName publishQueueTableName) {

        final String sql = format(INSERT_INTO_PUBLISH_TABLE_QUERY_PATTERN, publishQueueTableName.getTableName());
        try (final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            preparedStatement.setObject(1, eventId);
            preparedStatement.setObject(2, toSqlTimestamp(queuedAt));

            preparedStatement.executeUpdate();

        } catch (final SQLException e) {
            throw new PublishedEventException(format("Failed to add eventId '%s' to %s table", eventId, publishQueueTableName.getTableName()), e);
        }
    }

    @Transactional(MANDATORY)
    public Optional<UUID> popNextEventId(final PublishQueueTableName publishQueueTableName) {

        final String sql = format(SELECT_FROM_PUBLISH_TABLE_QUERY_PATTERN, publishQueueTableName.getTableName());
        try (final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(sql);
             final ResultSet resultSet = preparedStatement.executeQuery()) {

            if (resultSet.next()) {
                final UUID eventLogId = fromString(resultSet.getString("event_log_id"));

                deletePublishQueueRow(eventLogId, publishQueueTableName, connection);

                return of(eventLogId);
            }
        } catch (final SQLException e) {
            throw new PublishedEventException(format("Failed to read event_id from %s table", publishQueueTableName.getTableName()), e);
        }

        return empty();
    }


    @Transactional(MANDATORY)
    public int getSizeOfQueue(final PublishQueueTableName publishQueueTableName) {
        final String sql = format(COUNT_ROWS_QUERY_PATTERN, publishQueueTableName.getTableName());
        try (final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(sql);
             final ResultSet resultSet = preparedStatement.executeQuery()) {

            if (resultSet.next()) {
                return resultSet.getInt(1);
            } 
        } catch (final SQLException e) {
            throw new PublishedEventException(format("Failed to count rows of %s table", publishQueueTableName.getTableName()), e);
        }

        throw new PublishedEventException(format("Query '%s' returned no results", sql));
    }

    private void deletePublishQueueRow(final UUID eventLogId, final PublishQueueTableName publishQueueTableName, final Connection connection) throws SQLException {

        final String sql = format(DELETE_FROM_PUBLISH_TABLE_QUERY_PATTERN, publishQueueTableName.getTableName());
        try (final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, eventLogId);
            preparedStatement.executeUpdate();
        }
    }
}
