package uk.gov.justice.services.event.buffer.core.repository.streamerror;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.UUID;
import javax.inject.Inject;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.jdbc.persistence.JdbcRepositoryException;

import static java.lang.String.format;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.toSqlTimestamp;

public class StreamStatusErrorPersistence {

    private static final String UNMARK_STREAM_AS_ERRORED_SQL = """
                    UPDATE stream_status
                    SET stream_error_id = NULL,
                        stream_error_position = NULL
                    WHERE stream_id = ?
                    AND source = ?
                    AND component = ?
                """;

    private static final String UPDATE_STREAM_UPDATED_AT_IF_ERROR_IS_SAME = """
                UPDATE stream_status
                SET updated_at = ?
                WHERE stream_id = ?
                AND source = ?
                AND component = ?
                AND position = ?
                AND updated_at  = ?
            """;

    private static final String UPDATE_STREAM_STATUS_ERROR_DETAILS = """
                UPDATE stream_status
                SET stream_error_id = ?,
                    stream_error_position=?,
                    updated_at = ?
                WHERE stream_id = ?
                AND source = ?
                AND component = ?
            """;

    private static final String SELECT_FOR_UPDATE_SQL = """
            SELECT
                position
            FROM
                stream_status
            WHERE stream_id = ?
            AND source = ?
            AND component = ?
            FOR NO KEY UPDATE
            """;

    private static final long INITIAL_POSITION_ON_ERROR = 0L;

    @Inject
    private UtcClock clock;

    public void markStreamAsErrored(
            final UUID streamId,
            final UUID streamErrorId,
            final Long errorPosition,
            final String componentName,
            final String source,
            final Connection connection) {

        try (final PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_STREAM_STATUS_ERROR_DETAILS)) {

            final Timestamp updatedAtTimestamp = toSqlTimestamp(clock.now());

            preparedStatement.setObject(1, streamErrorId);
            preparedStatement.setLong(2, errorPosition);
            preparedStatement.setTimestamp(3, updatedAtTimestamp);
            preparedStatement.setObject(4, streamId);
            preparedStatement.setString(5, source);
            preparedStatement.setString(6, componentName);
            preparedStatement.executeUpdate();
        } catch (final SQLException e) {
            throw new JdbcRepositoryException(
                    format("Failed to mark stream as errored in stream_status table. streamId: '%s', component: '%s', streamErrorId: '%s' positionInStream: %s",
                            streamId,
                            componentName,
                            streamErrorId,
                            errorPosition),
                    e);
        }
    }

    public void unmarkStreamStatusAsErrored(
            final UUID streamId,
            final String source,
            final String componentName,
            final Connection connection) {

        try (final PreparedStatement preparedStatement = connection.prepareStatement(UNMARK_STREAM_AS_ERRORED_SQL)) {
            preparedStatement.setObject(1, streamId);
            preparedStatement.setString(2, source);
            preparedStatement.setString(3, componentName);
            preparedStatement.executeUpdate();
        } catch (final SQLException e) {
            throw new JdbcRepositoryException(format("Failed to unmark stream as errored in stream_status table. streamId: '%s'", streamId), e);
        }
    }

    public Long lockStreamForUpdate(final UUID streamId, final String source, final String component, final Connection connection) {

        try (final PreparedStatement preparedStatement = connection.prepareStatement(SELECT_FOR_UPDATE_SQL)) {
            preparedStatement.setObject(1, streamId);
            preparedStatement.setString(2, source);
            preparedStatement.setString(3, component);

            try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getLong("position");
                }

                throw new StreamNotFoundException(format(
                        "Failed to lock row in stream_status table. Stream with stream_id '%s', source '%s' and component '%s' does not exist",
                        streamId,
                        source,
                        component));
            }

        } catch (final SQLException e) {
            throw new StreamErrorHandlingException(format("Failed to lock row in stream_status table: streamId '%s', source '%s', component '%s'", streamId, source, component), e);
        }
    }


    public int updateStreamStatusUpdatedAtForSameError(final StreamError newStreamError, final long lastStreamPosition, final Timestamp previousUpdateAtTimestamp, final Connection connection) {
        final StreamErrorDetails streamErrorDetails = newStreamError.streamErrorDetails();
        final ZonedDateTime updatedAt = clock.now();

        try (final PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_STREAM_UPDATED_AT_IF_ERROR_IS_SAME)) {
            final Timestamp updatedAtTimestamp = toSqlTimestamp(updatedAt);

            preparedStatement.setObject(1, updatedAtTimestamp);
            preparedStatement.setObject(2, streamErrorDetails.streamId());
            preparedStatement.setString(3, streamErrorDetails.source());
            preparedStatement.setString(4, streamErrorDetails.componentName());
            preparedStatement.setObject(5, lastStreamPosition);
            preparedStatement.setObject(6, previousUpdateAtTimestamp);

            return preparedStatement.executeUpdate();
        } catch (final SQLException e) {
            throw new StreamErrorHandlingException(format(
                    "Failed to update Stream Status updated at. streamId: '%s', source: '%s, component: '%s'",
                    streamErrorDetails.streamId(),
                    streamErrorDetails.source(),
                    streamErrorDetails.componentName()
            ), e);
        }
    }
}
