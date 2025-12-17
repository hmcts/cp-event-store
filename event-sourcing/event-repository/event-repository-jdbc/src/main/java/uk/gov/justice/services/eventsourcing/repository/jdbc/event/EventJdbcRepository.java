package uk.gov.justice.services.eventsourcing.repository.jdbc.event;


import static java.lang.String.format;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;
import static java.util.UUID.fromString;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.fromSqlTimestamp;

import uk.gov.justice.services.eventsourcing.repository.jdbc.EventInsertionStrategy;
import uk.gov.justice.services.eventsourcing.repository.jdbc.exception.InvalidPositionException;
import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;
import uk.gov.justice.services.jdbc.persistence.JdbcRepositoryException;
import uk.gov.justice.services.jdbc.persistence.JdbcResultSetStreamer;
import uk.gov.justice.services.jdbc.persistence.PreparedStatementWrapper;
import uk.gov.justice.services.jdbc.persistence.PreparedStatementWrapperFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.inject.Inject;
import javax.sql.DataSource;

import org.slf4j.Logger;

/**
 * JDBC based repository for event log records.
 */
public class EventJdbcRepository {

    /**
     * Statements
     */
    static final String SQL_FIND_ALL = "SELECT * FROM event_log ORDER BY position_in_stream ASC";
    static final String SQL_FIND_BY_ID = "SELECT stream_id, position_in_stream, name, payload, metadata, date_created FROM event_log WHERE id = ?";
    static final String SQL_FIND_BY_STREAM_ID = "SELECT * FROM event_log WHERE stream_id=? ORDER BY position_in_stream ASC";
    static final String SQL_FIND_BY_STREAM_ID_AND_POSITION = "SELECT * FROM event_log WHERE stream_id=? AND position_in_stream>=? ORDER BY position_in_stream ASC";
    static final String SQL_FIND_BY_STREAM_ID_AND_POSITION_BY_PAGE = "SELECT * FROM event_log WHERE stream_id=? AND position_in_stream>=? ORDER BY position_in_stream ASC LIMIT ?";
    static final String SQL_FIND_LATEST_POSITION = "SELECT MAX(position_in_stream) FROM event_log WHERE stream_id=?";
    static final String SQL_DISTINCT_STREAM_ID = "SELECT DISTINCT stream_id FROM event_log";
    static final String SQL_DELETE_STREAM = "DELETE FROM event_log t WHERE t.stream_id=?";

    private static final long NO_EXISTING_VERSION = 0L;

    @Inject
    private EventInsertionStrategy eventInsertionStrategy;

    @Inject
    private JdbcResultSetStreamer jdbcResultSetStreamer;

    @Inject
    private PreparedStatementWrapperFactory preparedStatementWrapperFactory;

    @Inject
    private EventStoreDataSourceProvider eventStoreDataSourceProvider;

    @Inject
    private Logger logger;

    /**
     * Insert the given event into the event log.
     *
     * @param event the event to insert
     * @throws InvalidPositionException if the version already exists or is null.
     */
    public void insert(final Event event) throws InvalidPositionException {

        final DataSource dataSource = eventStoreDataSourceProvider.getDefaultDataSource();

        try (final PreparedStatementWrapper preparedStatementWrapper = preparedStatementWrapperFactory.preparedStatementWrapperOf(dataSource, eventInsertionStrategy.insertStatement())) {
            eventInsertionStrategy.insert(preparedStatementWrapper, event);
        } catch (final SQLException e) {
            logger.error("Error persisting event to the database", e);
            throw new JdbcRepositoryException(format("Exception while storing sequence %s of stream %s",
                    event.getPositionInStream(), event.getStreamId()), e);
        }
    }

    public Optional<Event> findById(final UUID id) {

        final DataSource dataSource = eventStoreDataSourceProvider.getDefaultDataSource();

        try (final Connection connection = dataSource.getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(SQL_FIND_BY_ID)) {

            preparedStatement.setObject(1, id);

            try (final ResultSet resultSet = preparedStatement.executeQuery()) {

                if (resultSet.next()) {
                    final UUID streamId = fromString(resultSet.getString("stream_id"));
                    final Long positionInStream = resultSet.getObject("position_in_stream", Long.class);
                    final String name = resultSet.getString("name");
                    final String metadata = resultSet.getString("metadata");
                    final String payload = resultSet.getString("payload");
                    final ZonedDateTime createdAt = fromSqlTimestamp(resultSet.getTimestamp("date_created"));

                    return of(new Event(
                            id,
                            streamId,
                            positionInStream,
                            name,
                            metadata,
                            payload,
                            createdAt)
                    );
                }
            }
        } catch (final SQLException e) {
            final String message = format("Failed to get event with id '%s'", id);
            logger.error(message, e);
            throw new JdbcRepositoryException(message, e);
        }

        return empty();
    }

    /**
     * Returns a Stream of {@link Event} for the given stream streamId.
     *
     * @param streamId streamId of the stream.
     * @return a stream of {@link Event}. Never returns null.
     */
    public Stream<Event> findByStreamIdOrderByPositionAsc(final UUID streamId) {

        final DataSource dataSource = eventStoreDataSourceProvider.getDefaultDataSource();

        try {
            final PreparedStatementWrapper preparedStatementWrapper = preparedStatementWrapperFactory.preparedStatementWrapperOf(dataSource, SQL_FIND_BY_STREAM_ID);
            preparedStatementWrapper.setObject(1, streamId);

            return jdbcResultSetStreamer.streamOf(preparedStatementWrapper, asEvent());
        } catch (final SQLException e) {
            logger.warn("Failed to read stream {}", streamId, e);
            throw new JdbcRepositoryException(format("Exception while reading stream %s", streamId), e);
        }
    }

    /**
     * Returns a Stream of {@link Event} for the given stream streamId starting from the given
     * version.
     *
     * @param streamId streamId of the stream.
     * @param position the position to read from.
     * @return a stream of {@link Event}. Never returns null.
     */
    public Stream<Event> findByStreamIdFromPositionOrderByPositionAsc(final UUID streamId,
                                                                      final Long position) {

        final DataSource dataSource = eventStoreDataSourceProvider.getDefaultDataSource();

        try {
            final PreparedStatementWrapper preparedStatementWrapper = preparedStatementWrapperFactory.preparedStatementWrapperOf(dataSource, SQL_FIND_BY_STREAM_ID_AND_POSITION);
            preparedStatementWrapper.setObject(1, streamId);
            preparedStatementWrapper.setLong(2, position);

            return jdbcResultSetStreamer.streamOf(preparedStatementWrapper, asEvent());
        } catch (final SQLException e) {
            logger.warn("Failed to read stream {}", streamId, e);
            throw new JdbcRepositoryException(format("Exception while reading stream %s", streamId), e);
        }
    }

    public Stream<Event> findByStreamIdFromPositionOrderByPositionAsc(final UUID streamId,
                                                                      final Long versionFrom,
                                                                      final Integer pageSize) {

        final DataSource dataSource = eventStoreDataSourceProvider.getDefaultDataSource();

        try {
            final PreparedStatementWrapper preparedStatementWrapper = preparedStatementWrapperFactory.preparedStatementWrapperOf(dataSource, SQL_FIND_BY_STREAM_ID_AND_POSITION_BY_PAGE);
            preparedStatementWrapper.setObject(1, streamId);
            preparedStatementWrapper.setLong(2, versionFrom);
            preparedStatementWrapper.setInt(3, pageSize);

            return jdbcResultSetStreamer.streamOf(preparedStatementWrapper, asEvent());
        } catch (final SQLException e) {
            logger.error("Failed to read stream {}", streamId, e);
            throw new JdbcRepositoryException(format("Exception while reading stream %s", streamId), e);
        }
    }

    /**
     * Returns a Stream of {@link Event}
     *
     * @return a stream of {@link Event}. Never returns null.
     */
    public Stream<Event> findAll() {

        final DataSource dataSource = eventStoreDataSourceProvider.getDefaultDataSource();

        try {
            return jdbcResultSetStreamer
                    .streamOf(preparedStatementWrapperFactory
                            .preparedStatementWrapperOf(dataSource, SQL_FIND_ALL), asEvent());
        } catch (final SQLException e) {
            throw new JdbcRepositoryException("Exception while reading stream", e);
        }
    }

    /**
     * Returns the current position for the given stream streamId.
     *
     * @param streamId streamId of the stream.
     * @return current position streamId for the stream.  Returns 0 if stream doesn't exist.
     */
    public Long getStreamSize(final UUID streamId) {

        final DataSource dataSource = eventStoreDataSourceProvider.getDefaultDataSource();

        try (final PreparedStatementWrapper preparedStatementWrapper = preparedStatementWrapperFactory.preparedStatementWrapperOf(dataSource, SQL_FIND_LATEST_POSITION)) {
            preparedStatementWrapper.setObject(1, streamId);

            final ResultSet resultSet = preparedStatementWrapper.executeQuery();

            if (resultSet.next()) {
                return resultSet.getLong(1);
            }

        } catch (final SQLException e) {
            logger.warn("Failed to read stream {}", streamId, e);
            throw new JdbcRepositoryException(format("Exception while reading stream %s", streamId), e);
        }

        return NO_EXISTING_VERSION;
    }


    /**
     * Returns stream of event stream ids
     *
     * @return event stream ids
     */
    public Stream<UUID> getStreamIds() {

        final DataSource dataSource = eventStoreDataSourceProvider.getDefaultDataSource();

        try {
            final PreparedStatementWrapper preparedStatementWrapper = preparedStatementWrapperFactory.preparedStatementWrapperOf(dataSource, SQL_DISTINCT_STREAM_ID);
            return streamFrom(preparedStatementWrapper);
        } catch (final SQLException e) {
            throw new JdbcRepositoryException("Exception while reading stream", e);
        }

    }

    public void clear(final UUID streamId) {

        final long eventCount = getStreamSize(streamId);

        final DataSource dataSource = eventStoreDataSourceProvider.getDefaultDataSource();

        try (final PreparedStatementWrapper preparedStatementWrapper = preparedStatementWrapperFactory.preparedStatementWrapperOf(
                dataSource,
                SQL_DELETE_STREAM)) {

            preparedStatementWrapper.setObject(1, streamId);

            final int deletedRows = preparedStatementWrapper.executeUpdate();

            if (deletedRows != eventCount) {
                // Rollback, something went wrong
                throw new JdbcRepositoryException(format("Exception while deleting stream %s, expected %d rows to be updated but was %d", streamId, eventCount, deletedRows));
            }
        } catch (final SQLException e) {
            throw new JdbcRepositoryException(format("Exception while deleting stream %s", streamId), e);
        }
    }

    private Stream<UUID> streamFrom(final PreparedStatementWrapper preparedStatementWrapper) throws SQLException {
        return jdbcResultSetStreamer.streamOf(preparedStatementWrapper, resultSet -> {
            try {
                return (UUID) resultSet.getObject("stream_id");
            } catch (final SQLException e) {
                preparedStatementWrapper.close();
                throw new JdbcRepositoryException(e);
            }
        });
    }

    private Function<ResultSet, Event> asEvent() {
        return resultSet -> {
            try {
                return new Event((UUID) resultSet.getObject("id"),
                        (UUID) resultSet.getObject("stream_id"),
                        resultSet.getObject("position_in_stream", Long.class),
                        resultSet.getString("name"),
                        resultSet.getString("metadata"),
                        resultSet.getString("payload"),
                        fromSqlTimestamp(resultSet.getTimestamp("date_created")),
                        ofNullable(resultSet.getObject("event_number", Long.class))
                );
            } catch (final SQLException e) {
                throw new JdbcRepositoryException(e);
            }
        };
    }

}
