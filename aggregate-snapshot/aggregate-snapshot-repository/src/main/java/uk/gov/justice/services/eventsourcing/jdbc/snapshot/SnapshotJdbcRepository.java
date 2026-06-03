package uk.gov.justice.services.eventsourcing.jdbc.snapshot;

import static java.lang.String.format;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.fromSqlTimestamp;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.toSqlTimestamp;

import uk.gov.justice.domain.aggregate.Aggregate;
import uk.gov.justice.domain.snapshot.AggregateSnapshot;
import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;
import uk.gov.justice.services.jdbc.persistence.JdbcRepositoryException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.slf4j.Logger;

/**
 * JDBC based repository for snapshot records.
 */
@ApplicationScoped
public class SnapshotJdbcRepository implements SnapshotRepository {

    protected static final String READING_STREAM_EXCEPTION = "Exception while reading stream %s";

    private static final String COL_STREAM_ID = "stream_id";
    private static final String COL_VERSION_ID = "version_id";
    private static final String COL_TYPE = "type";
    private static final String COL_AGGREGATE = "aggregate";
    private static final String COL_CREATED_AT = "created_at";
    private static final String SQL_FIND_LATEST_BY_STREAM_ID = "SELECT * FROM snapshot WHERE stream_id=? AND type=? ORDER BY version_id DESC LIMIT 1";
    private static final String SQL_UPSERT_SNAPSHOT = "INSERT INTO snapshot AS s (stream_id, version_id, type, aggregate, created_at ) VALUES(?, ?, ?, ?, ?) ON CONFLICT ON CONSTRAINT stream_id_version_id_type DO UPDATE SET aggregate =?, created_at = ? WHERE s.created_at<=?";
    private static final String DELETE_ALL_SNAPSHOTS_FOR_STREAM_ID_AND_CLASS = "delete from snapshot where stream_id =? and type=?";
    private static final String DELETE_ALL_SNAPSHOTS_OF_STREAM_ID_AND_CLASS_AND_LESS_THAN_POSITION_IN_STREAM = "delete from snapshot where stream_id =? and type=? and version_id<?";
    private static final String SQL_CURRENT_SNAPSHOT_VERSION_ID = "SELECT version_id FROM snapshot WHERE stream_id=? AND type=? ORDER BY version_id DESC LIMIT 1";

    // using 'stream_id =? and type=? and created_at <=?' may give better outcome and is backward compatible with the current behavious
    private static final String DELETE_SNAPSHOTS = "delete from snapshot where stream_id =? and type=? and version_id <=? and created_at <=? ";

    @Inject
    private EventStoreDataSourceProvider eventStoreDataSourceProvider;

    @Inject
    private Logger logger;

    @Inject
    private UtcClock clock;

    @Override
    public boolean storeSnapshot(final AggregateSnapshot aggregateSnapshot) {

        try (final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
             final PreparedStatement ps = connection.prepareStatement(SQL_UPSERT_SNAPSHOT)) {
            final Timestamp now = toSqlTimestamp(clock.now());

            ps.setObject(1, aggregateSnapshot.getStreamId());
            ps.setLong(2, aggregateSnapshot.getPositionInStream());
            ps.setString(3, aggregateSnapshot.getType());
            ps.setBytes(4, aggregateSnapshot.getAggregateByteRepresentation());
            ps.setTimestamp(5, now);
            ps.setBytes(6, aggregateSnapshot.getAggregateByteRepresentation());
            ps.setTimestamp(7, now);
            ps.setTimestamp(8, now);

            ps.executeUpdate();

            return true;
        } catch (final SQLException e) {
            logger.error("Error while storing a snapshot for {} at version {}", aggregateSnapshot.getStreamId(), aggregateSnapshot.getPositionInStream(), e);
        }

        return false;
    }

    @Override
    public <T extends Aggregate> Optional<AggregateSnapshot<T>> getLatestSnapshot(final UUID streamId, final Class<T> clazz) {

        try (final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(SQL_FIND_LATEST_BY_STREAM_ID)) {

            preparedStatement.setObject(1, streamId);
            preparedStatement.setObject(2, clazz.getName());

            return extractResults(preparedStatement);

        } catch (final SQLException e) {
            logger.error(format(READING_STREAM_EXCEPTION, streamId), e);
        }
        return Optional.empty();
    }

    @Override
    public <T extends Aggregate> void removeAllSnapshots(final UUID streamId, final Class<T> clazz) {
        try (final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
             final PreparedStatement ps = connection.prepareStatement(DELETE_ALL_SNAPSHOTS_FOR_STREAM_ID_AND_CLASS)) {
            ps.setObject(1, streamId);
            ps.setString(2, clazz.getName());
            ps.executeUpdate();
        } catch (final SQLException e) {
            logger.error(format("Exception while removing snapshots %s of stream %s", clazz, streamId), e);
        }
    }

    @Override
    public <T extends Aggregate> int removeSnapshots(final UUID streamId, final Class<T> clazz, final long positionInStream, final ZonedDateTime createdAt) {
        try (final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
             final PreparedStatement ps = connection.prepareStatement(DELETE_SNAPSHOTS)) {
            ps.setObject(1, streamId);
            ps.setString(2, clazz.getName());
            ps.setLong(3, positionInStream);
            ps.setTimestamp(4, toSqlTimestamp(createdAt));
            return ps.executeUpdate();
        } catch (final SQLException e) {
            logger.error("Exception while removing snapshots %s of stream %s".formatted(clazz, streamId), e);
        }
        return 0;
    }

    @Override
    public <T extends Aggregate> void removeAllSnapshotsOlderThan(final AggregateSnapshot aggregateSnapshot) {
        try (final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
             final PreparedStatement ps = connection.prepareStatement(DELETE_ALL_SNAPSHOTS_OF_STREAM_ID_AND_CLASS_AND_LESS_THAN_POSITION_IN_STREAM)) {
            ps.setObject(1, aggregateSnapshot.getStreamId());
            ps.setString(2, aggregateSnapshot.getType());
            ps.setLong(3, aggregateSnapshot.getPositionInStream());
            ps.executeUpdate();
        } catch (final SQLException e) {
            logger.error(format("Exception while removing old snapshots %s of stream %s, version_id less than %s", aggregateSnapshot.getType(), aggregateSnapshot.getStreamId(), aggregateSnapshot.getPositionInStream()), e);
        }
    }

    @Override
    public <T extends Aggregate> long getLatestSnapshotVersion(final UUID streamId, final Class<T> clazz) {

        try (final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(SQL_CURRENT_SNAPSHOT_VERSION_ID)) {
            preparedStatement.setObject(1, streamId);
            preparedStatement.setObject(2, clazz.getName());

            try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getLong(1);
                }
                return 0;
            }
        } catch (final SQLException e) {
            throw new JdbcRepositoryException(format(READING_STREAM_EXCEPTION, streamId), e);
        }
    }

    public AggregateSnapshot entityFrom(final ResultSet resultSet) throws SQLException {
        return new AggregateSnapshot(
                (UUID) resultSet.getObject(COL_STREAM_ID),
                resultSet.getLong(COL_VERSION_ID),
                resultSet.getString(COL_TYPE),
                resultSet.getBytes(COL_AGGREGATE),
                fromSqlTimestamp(resultSet.getTimestamp(COL_CREATED_AT)));
    }

    @SuppressWarnings("unchecked")
    private <T extends Aggregate> Optional<AggregateSnapshot<T>> extractResults(final PreparedStatement preparedStatement) throws SQLException {

        try (final ResultSet resultSet = preparedStatement.executeQuery()) {
            if (resultSet.next()) {
                return Optional.of(entityFrom(resultSet));
            }
        }
        return Optional.empty();
    }
}
