package uk.gov.justice.services.event.buffer.core.repository.subscription;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.inject.Inject;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;

import static java.lang.String.format;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.fromSqlTimestamp;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.toSqlTimestamp;

@SuppressWarnings("java:S1192")
public class NewStreamStatusRepository {

    private static final long INITIAL_POSITION_IN_STREAM = 0L;

    private static final String INSERT_OR_DO_NOTHING_SQL = """
            INSERT INTO stream_status (
                stream_id,
                position,
                source,
                component,
                updated_at,
                latest_known_position,
                is_up_to_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (stream_id, source, component) DO NOTHING
            """;
    private static final String SELECT_SQL = """
                SELECT
                    stream_id,
                    source,
                    component,
                    position,
                    stream_error_id,
                    stream_error_position,
                    updated_at,
                    latest_known_position,
                    is_up_to_date
                FROM stream_status
                WHERE stream_id = ?
                AND source = ?
                AND component = ?
            """;
    private static final String FIND_ALL_SQL = """
                SELECT
                    stream_id,
                    source,
                    component,
                    position,
                    stream_error_id,
                    stream_error_position,
                    updated_at,
                    latest_known_position,
                    is_up_to_date
                FROM stream_status
            """;
    private static final String LOCK_AND_GET_POSITIONS_SQL = """
                SELECT
                        position,
                        latest_known_position
                FROM stream_status
                WHERE stream_id = ?
                AND source = ?
                AND component = ?
                FOR UPDATE
                """;
    private static final String UPDATE_CURRENT_POSITION_IN_STREAM = """
                UPDATE stream_status
                SET position = ?
                WHERE stream_id = ?
                AND source = ?
                AND component = ?
            """;
    private static final String UPDATE_LATEST_KNOWN_POSITION_IN_STREAM = """
                UPDATE stream_status
                SET latest_known_position = ?,
                is_up_to_date = ?
                WHERE stream_id = ?
                AND source = ?
                AND component = ?
            """;
    private static final String SET_IS_UP_TO_DATE_SQL = """
                UPDATE stream_status
                SET is_up_to_date = ?
                WHERE stream_id = ?
                AND source = ?
                AND component = ?
                """;
    @Inject
    private NewStreamStatusRowMapper streamStatusRowMapper;

    @Inject
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    public int insertIfNotExists(
            final UUID streamId,
            final String source,
            final String componentName,
            final ZonedDateTime updatedAt,
            final boolean isUpToDate) {

        final Timestamp updatedAtTimestamp = toSqlTimestamp(updatedAt);

        try (final Connection connection = viewStoreJdbcDataSourceProvider.getDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(INSERT_OR_DO_NOTHING_SQL)) {

            preparedStatement.setObject(1, streamId);
            preparedStatement.setLong(2, INITIAL_POSITION_IN_STREAM);
            preparedStatement.setString(3, source);
            preparedStatement.setString(4, componentName);
            preparedStatement.setTimestamp(5, updatedAtTimestamp);
            preparedStatement.setLong(6, INITIAL_POSITION_IN_STREAM);
            preparedStatement.setBoolean(7, isUpToDate);

            return preparedStatement.executeUpdate();
        } catch (final SQLException e) {
            throw new StreamStatusException(format(
                    "Failed to insert into stream_status table; stream_id '%s', source '%s', component '%s",
                    streamId,
                    source,
                    componentName),
                    e);
        }
    }

    public List<StreamStatus> findAll() {

        try (final Connection connection = viewStoreJdbcDataSourceProvider.getDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(FIND_ALL_SQL)) {

            try (final ResultSet resultSet = preparedStatement.executeQuery()) {

                final ArrayList<StreamStatus> streamStatuses = new ArrayList<>();

                while (resultSet.next()) {
                    final UUID streamId = resultSet.getObject("stream_id", UUID.class);
                    final String source = resultSet.getString("source");
                    final String componentName = resultSet.getString("component");
                    final Long position = resultSet.getLong("position");
                    final Optional<UUID> streamErrorId = ofNullable((UUID) resultSet.getObject("stream_error_id"));
                    final Optional<Long> streamErrorPosition = ofNullable(resultSet.getObject("stream_error_position", Long.class));
                    final ZonedDateTime updatedAt = fromSqlTimestamp(resultSet.getTimestamp("updated_at"));
                    final Long latestKnownPosition = resultSet.getLong("latest_known_position");
                    final Boolean isUpToDate = resultSet.getBoolean("is_up_to_date");

                    final StreamStatus streamStatus = new StreamStatus(
                            streamId,
                            position,
                            source,
                            componentName,
                            streamErrorId,
                            streamErrorPosition,
                            updatedAt,
                            latestKnownPosition,
                            isUpToDate
                    );

                    streamStatuses.add(streamStatus);
                }

                return streamStatuses;
            }

        } catch (final SQLException e) {
            throw new StreamStatusException("Failed to find all from stream_status table", e);
        }
    }

    public StreamPositions lockRowAndGetPositions(final UUID streamId, final String source, final String componentName, final long incomingEventPosition) {

        try (final Connection connection = viewStoreJdbcDataSourceProvider.getDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(LOCK_AND_GET_POSITIONS_SQL)) {

            preparedStatement.setObject(1, streamId);
            preparedStatement.setString(2, source);
            preparedStatement.setString(3, componentName);

            try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    final long currentStreamPosition = resultSet.getLong("position");
                    final long latestKnownPosition = resultSet.getLong("latest_known_position");

                    return new StreamPositions(
                            incomingEventPosition,
                            currentStreamPosition,
                            latestKnownPosition
                    );
                }
            }

            throw new StreamStatusException(format(
                    "Failed to select for update from stream_status table; No stream not found with stream_id '%s', source '%s', component '%s",
                    streamId,
                    source,
                    componentName));

        } catch (final SQLException e) {
            throw new StreamStatusException(format(
                    "Failed to select for update from stream_status table; stream_id '%s', source '%s', component '%s",
                    streamId,
                    source,
                    componentName),
                    e);
        }
    }

    public Optional<StreamStatus> find(final UUID streamId, String source, final String componentName) {

        try (final Connection connection = viewStoreJdbcDataSourceProvider.getDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(SELECT_SQL)) {

            preparedStatement.setObject(1, streamId);
            preparedStatement.setString(2, source);
            preparedStatement.setString(3, componentName);

            try (final ResultSet resultSet = preparedStatement.executeQuery()) {

                if (resultSet.next()) {
                    return of(streamStatusRowMapper.mapRow(resultSet));
                }
            }

        } catch (final SQLException e) {
            throw new StreamStatusException(format(
                    "Failed to select from stream_status table; stream_id '%s', source '%s', component '%s",
                    streamId,
                    source,
                    componentName),
                    e);
        }

        return empty();
    }

    public void updateCurrentPosition(final UUID streamId, final String source, final String componentName, final long currentStreamPosition) {

        try (final Connection connection = viewStoreJdbcDataSourceProvider.getDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_CURRENT_POSITION_IN_STREAM)) {

            preparedStatement.setLong(1, currentStreamPosition);
            preparedStatement.setObject(2, streamId);
            preparedStatement.setString(3, source);
            preparedStatement.setString(4, componentName);

            preparedStatement.executeUpdate();
        } catch (final SQLException e) {
            throw new StreamStatusException(format(
                    "Failed to update stream_status position; stream_id '%s', source '%s', component '%s', new_stream_position '%d'",
                    streamId,
                    source,
                    componentName,
                    currentStreamPosition),
                    e);
        }
    }

    public void updateLatestKnownPositionAndIsUpToDateToFalse(final UUID streamId, final String source, final String componentName, final long latestKnownPosition) {

        try (final Connection connection = viewStoreJdbcDataSourceProvider.getDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_LATEST_KNOWN_POSITION_IN_STREAM)) {

            preparedStatement.setLong(1, latestKnownPosition);
            preparedStatement.setBoolean(2, false);
            preparedStatement.setObject(3, streamId);
            preparedStatement.setString(4, source);
            preparedStatement.setString(5, componentName);

            preparedStatement.executeUpdate();
        } catch (final SQLException e) {
            throw new StreamStatusException(format(
                    "Failed to update stream_status latest_known_position; stream_id '%s', source '%s', component '%s', latestKnownPosition '%d'",
                    streamId,
                    source,
                    componentName,
                    latestKnownPosition),
                    e);
        }
    }

    public void setUpToDate(final boolean upToDate, final UUID streamId, final String source, final String componentName) {

        try (final Connection connection = viewStoreJdbcDataSourceProvider.getDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(SET_IS_UP_TO_DATE_SQL)) {

            preparedStatement.setBoolean(1, upToDate);
            preparedStatement.setObject(2, streamId);
            preparedStatement.setString(3, source);
            preparedStatement.setString(4, componentName);

            preparedStatement.executeUpdate();
        } catch (final SQLException e) {
            throw new StreamStatusException(format(
                    "Failed to set is_up_to_date on stream_status; stream_id '%s', source '%s', component '%s'",
                    streamId,
                    source,
                    componentName),
                    e);
        }
    }
}
