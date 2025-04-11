package uk.gov.justice.services.event.buffer.core.repository.streamerror;

import static java.lang.String.format;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.toSqlTimestamp;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.jdbc.persistence.JdbcRepositoryException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.UUID;

import javax.inject.Inject;

public class StreamStatusErrorPersistence {

    private static final String UNMARK_STREAM_AS_ERRORED_SQL = """
                    UPDATE stream_status
                    SET stream_error_id = NULL,
                        stream_error_position = NULL
                    WHERE stream_id = ?
                    AND source = ?
                    AND component = ?
                """;

    private static final String UPSERT_STREAM_ERROR_SQL = """
            INSERT INTO stream_status (
                stream_id,
                position,
                source,
                component,
                stream_error_id,
                stream_error_position,
                updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (stream_id, source, component)
            DO UPDATE
            SET stream_error_id = ?, stream_error_position = ?, updated_at = ?""";

    private static final String SELECT_FOR_UPDATE_SQL = """
            SELECT
                position
            FROM
                stream_status
            WHERE stream_id = ?
            AND source = ?
            AND component = ?
            FOR UPDATE
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

        final ZonedDateTime updatedAt = clock.now();

        try (final PreparedStatement preparedStatement = connection.prepareStatement(UPSERT_STREAM_ERROR_SQL)) {

            final Timestamp updatedAtTimestamp = toSqlTimestamp(updatedAt);

            preparedStatement.setObject(1, streamId);
            preparedStatement.setLong(2, INITIAL_POSITION_ON_ERROR);
            preparedStatement.setString(3, source);
            preparedStatement.setString(4, componentName);
            preparedStatement.setObject(5, streamErrorId);
            preparedStatement.setLong(6, errorPosition);
            preparedStatement.setTimestamp(7, updatedAtTimestamp);
            preparedStatement.setObject(8, streamErrorId);
            preparedStatement.setLong(9, errorPosition);
            preparedStatement.setTimestamp(10, updatedAtTimestamp);

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

            try(final ResultSet resultSet = preparedStatement.executeQuery()) {
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


}
