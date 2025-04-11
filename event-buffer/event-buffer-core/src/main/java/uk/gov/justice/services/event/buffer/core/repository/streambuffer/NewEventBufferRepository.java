package uk.gov.justice.services.event.buffer.core.repository.streambuffer;

import static java.lang.String.format;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.fromSqlTimestamp;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.toSqlTimestamp;

import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;

import javax.inject.Inject;

public class NewEventBufferRepository {

    private static final String SQL = """
            INSERT INTO stream_buffer (
                stream_id,
                position,
                event,
                source,
                component,
                buffered_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """;

    private static final String FIND_NEXT_FOR_STREAM_SQL = """
            SELECT position, event, buffered_at FROM stream_buffer
            WHERE stream_id = ?
            AND source = ?
            AND component = ?
            ORDER BY position ASC
            LIMIT 1
            """;

    private static final String REMOVE_FROM_BUFFER_SQL = """
            DELETE FROM stream_buffer
            WHERE stream_id = ?
            AND source = ?
            AND component = ?
            AND position = ?
            """;

    @Inject
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    public void buffer(final EventBufferEvent eventBufferEvent) {

        try (final Connection connection = viewStoreJdbcDataSourceProvider.getDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(SQL)) {
            preparedStatement.setObject(1, eventBufferEvent.getStreamId());
            preparedStatement.setLong(2, eventBufferEvent.getPosition());
            preparedStatement.setString(3, eventBufferEvent.getEvent());
            preparedStatement.setString(4, eventBufferEvent.getSource());
            preparedStatement.setString(5, eventBufferEvent.getComponent());
            preparedStatement.setTimestamp(6, toSqlTimestamp(eventBufferEvent.getBufferedAt()));

            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new EventBufferPersistenceException(
                    format("Failed to insert event into event-buffer table. streamId '%s'. source '%s', component '%s'",
                            eventBufferEvent.getStreamId(),
                            eventBufferEvent.getSource(),
                            eventBufferEvent.getComponent()),
                    e);
        }
    }


    public Optional<EventBufferEvent> findNextForStream(final UUID streamId, final String source, final String componentName) {

        try (final Connection connection = viewStoreJdbcDataSourceProvider.getDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(FIND_NEXT_FOR_STREAM_SQL)) {

            preparedStatement.setObject(1, streamId);
            preparedStatement.setString(2, source);
            preparedStatement.setString(3, componentName);

            try (final ResultSet resultSet = preparedStatement.executeQuery()) {

                if (resultSet.next()) {
                    final long position = resultSet.getLong("position");
                    final String event = resultSet.getString("event");
                    final ZonedDateTime bufferedAt = fromSqlTimestamp(resultSet.getTimestamp("buffered_at"));

                    final EventBufferEvent eventBufferEvent = new EventBufferEvent(
                            streamId,
                            position,
                            event,
                            source,
                            componentName,
                            bufferedAt
                    );

                    return of(eventBufferEvent);
                }

                return empty();
            }

        } catch (SQLException e) {
            throw new EventBufferPersistenceException("Ooops", e);
        }
    }

    public void removeFromBuffer(final EventBufferEvent eventBufferEvent) {

        try (final Connection connection = viewStoreJdbcDataSourceProvider.getDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(REMOVE_FROM_BUFFER_SQL)) {

            preparedStatement.setObject(1, eventBufferEvent.getStreamId());
            preparedStatement.setString(2, eventBufferEvent.getSource());
            preparedStatement.setString(3, eventBufferEvent.getComponent());
            preparedStatement.setLong(4, eventBufferEvent.getPosition());

            preparedStatement.executeUpdate();

        } catch (final SQLException e) {
            throw new EventBufferPersistenceException(
                    format("Failed to remove event from event-buffer table. streamId '%s'. source '%s', component '%s'",
                            eventBufferEvent.getStreamId(),
                            eventBufferEvent.getSource(),
                            eventBufferEvent.getComponent()),
                    e);
        }
    }

}
