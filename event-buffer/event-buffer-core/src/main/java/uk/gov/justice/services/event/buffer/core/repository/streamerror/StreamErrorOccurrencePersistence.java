package uk.gov.justice.services.event.buffer.core.repository.streamerror;

import static java.lang.String.format;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.fromSqlTimestamp;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.toSqlTimestamp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@SuppressWarnings("java:S1192")
public class StreamErrorOccurrencePersistence {

    private static final String INSERT_EXCEPTION_SQL = """
            INSERT INTO stream_error (
                id,
                hash,
                exception_message,
                cause_message,
                event_name,
                event_id,
                stream_id,
                position_in_stream,
                date_created,
                full_stack_trace,
                component,
                source,
                occurred_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (stream_id, component, source) DO NOTHING
            """;

    private static final String FIND_BY_ID_SQL = """
            SELECT
                hash,
                exception_message,
                cause_message,
                event_name,
                event_id,
                stream_id,
                position_in_stream,
                date_created,
                full_stack_trace,
                component,
                source,
                occurred_at
            FROM stream_error
            WHERE id = ?
            """;

    private static final String FIND_BY_STREAM_ID_SQL = """
            SELECT
                id,
                hash,
                exception_message,
                cause_message,
                event_name,
                event_id,
                position_in_stream,
                date_created,
                full_stack_trace,
                component,
                source,
                occurred_at
            FROM stream_error
            WHERE stream_id = ?
            """;
    private static final String FIND_ALL_SQL = """
            SELECT
                id,
                hash,
                exception_message,
                cause_message,
                event_name,
                event_id,
                stream_id,
                position_in_stream,
                date_created,
                full_stack_trace,
                component,
                source,
                occurred_at
            FROM stream_error
            """;
    private static final String DELETE_AND_RETURN_HASH_SQL = """
            DELETE FROM stream_error WHERE id = ? RETURNING hash
            """;
    private static final String ERRORS_EXIST_FOR_HASH_SQL = """
        SELECT 1
        FROM stream_error
        WHERE hash = ?
        LIMIT 1
        """;


    public int insert(final StreamErrorOccurrence streamErrorOccurrence, final Connection connection) throws SQLException {

        try (final PreparedStatement preparedStatement = connection.prepareStatement(INSERT_EXCEPTION_SQL)) {
            preparedStatement.setObject(1, streamErrorOccurrence.id());
            preparedStatement.setString(2, streamErrorOccurrence.hash());
            preparedStatement.setString(3, streamErrorOccurrence.exceptionMessage());
            preparedStatement.setString(4, streamErrorOccurrence.causeMessage().orElse(null));
            preparedStatement.setString(5, streamErrorOccurrence.eventName());
            preparedStatement.setObject(6, streamErrorOccurrence.eventId());
            preparedStatement.setObject(7, streamErrorOccurrence.streamId());
            preparedStatement.setLong(8, streamErrorOccurrence.positionInStream());
            preparedStatement.setTimestamp(9, toSqlTimestamp(streamErrorOccurrence.dateCreated()));
            preparedStatement.setString(10, streamErrorOccurrence.fullStackTrace());
            preparedStatement.setString(11, streamErrorOccurrence.componentName());
            preparedStatement.setString(12, streamErrorOccurrence.source());
            preparedStatement.setTimestamp(13, toSqlTimestamp(streamErrorOccurrence.occurredAt()));

            return preparedStatement.executeUpdate();
        }
    }

    public Optional<StreamErrorOccurrence> findById(final UUID id, final Connection connection) throws SQLException {

        try (final PreparedStatement preparedStatement = connection.prepareStatement(FIND_BY_ID_SQL)) {
            preparedStatement.setObject(1, id);
            try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    final String hash = resultSet.getString("hash");
                    final String exceptionMessage = resultSet.getString("exception_message");
                    final String causeMessage = resultSet.getString("cause_message");
                    final String eventName = resultSet.getString("event_name");
                    final UUID eventId = (UUID) resultSet.getObject("event_id");
                    final UUID streamId = (UUID) resultSet.getObject("stream_id");
                    final Long positionInStream = resultSet.getLong("position_in_stream");
                    final ZonedDateTime dateCreated = fromSqlTimestamp(resultSet.getTimestamp("date_created"));
                    final String fullStackTrace = resultSet.getString("full_stack_trace");
                    final String componentName = resultSet.getString("component");
                    final String source = resultSet.getString("source");
                    final ZonedDateTime occurredAt = fromSqlTimestamp(resultSet.getTimestamp("occurred_at"));

                    final StreamErrorOccurrence streamErrorOccurrence = new StreamErrorOccurrence(
                            id,
                            hash,
                            exceptionMessage,
                            ofNullable(causeMessage),
                            eventName,
                            eventId,
                            streamId,
                            positionInStream,
                            dateCreated,
                            fullStackTrace,
                            componentName,
                            source,
                            occurredAt
                    );

                    return of(streamErrorOccurrence);
                }

                return empty();
            }
        }
    }

    public List<StreamErrorOccurrence> findByStreamId(final UUID streamId, final Connection connection) throws SQLException {

        try (final PreparedStatement preparedStatement = connection.prepareStatement(FIND_BY_STREAM_ID_SQL)) {
            preparedStatement.setObject(1, streamId);
            try (final ResultSet resultSet = preparedStatement.executeQuery()) {

                final ArrayList<StreamErrorOccurrence> streamErrorOccurrenceList = new ArrayList<>();
                while (resultSet.next()) {
                    final UUID id = resultSet.getObject("id", UUID.class);
                    final String hash = resultSet.getString("hash");
                    final String exceptionMessage = resultSet.getString("exception_message");
                    final String causeMessage = resultSet.getString("cause_message");
                    final String eventName = resultSet.getString("event_name");
                    final UUID eventId = (UUID) resultSet.getObject("event_id");
                    final Long positionInStream = resultSet.getLong("position_in_stream");
                    final ZonedDateTime dateCreated = fromSqlTimestamp(resultSet.getTimestamp("date_created"));
                    final String fullStackTrace = resultSet.getString("full_stack_trace");
                    final String componentName = resultSet.getString("component");
                    final String source = resultSet.getString("source");
                    final ZonedDateTime occurredAt = fromSqlTimestamp(resultSet.getTimestamp("occurred_at"));

                    final StreamErrorOccurrence streamErrorOccurrence = new StreamErrorOccurrence(
                            id,
                            hash,
                            exceptionMessage,
                            ofNullable(causeMessage),
                            eventName,
                            eventId,
                            streamId,
                            positionInStream,
                            dateCreated,
                            fullStackTrace,
                            componentName,
                            source,
                            occurredAt
                    );

                    streamErrorOccurrenceList.add(streamErrorOccurrence);
                }

                return streamErrorOccurrenceList;
            }
        }
    }

    public List<StreamErrorOccurrence> findAll(final Connection connection) throws SQLException {

        try (final PreparedStatement preparedStatement = connection.prepareStatement(FIND_ALL_SQL)) {
            try (final ResultSet resultSet = preparedStatement.executeQuery()) {

                final ArrayList<StreamErrorOccurrence> streamErrorOccurrenceList = new ArrayList<>();
                while (resultSet.next()) {
                    final UUID streamErrorId = (UUID) resultSet.getObject("id");
                    final String hash = resultSet.getString("hash");
                    final String exceptionMessage = resultSet.getString("exception_message");
                    final String causeMessage = resultSet.getString("cause_message");
                    final String eventName = resultSet.getString("event_name");
                    final UUID eventId = (UUID) resultSet.getObject("event_id");
                    final UUID streamId = (UUID) resultSet.getObject("stream_id");
                    final Long positionInStream = resultSet.getLong("position_in_stream");
                    final ZonedDateTime dateCreated = fromSqlTimestamp(resultSet.getTimestamp("date_created"));
                    final String fullStackTrace = resultSet.getString("full_stack_trace");
                    final String componentName = resultSet.getString("component");
                    final String source = resultSet.getString("source");
                    final ZonedDateTime occurredAt = fromSqlTimestamp(resultSet.getTimestamp("occurred_at"));

                    final StreamErrorOccurrence streamErrorOccurrence = new StreamErrorOccurrence(
                            streamErrorId,
                            hash,
                            exceptionMessage,
                            ofNullable(causeMessage),
                            eventName,
                            eventId,
                            streamId,
                            positionInStream,
                            dateCreated,
                            fullStackTrace,
                            componentName,
                            source,
                            occurredAt
                    );

                    streamErrorOccurrenceList.add(streamErrorOccurrence);
                }

                return streamErrorOccurrenceList;
            }
        }
    }

    public String deleteErrorAndGetHash(final UUID streamErrorId, final Connection connection) throws SQLException {

        try(final PreparedStatement preparedStatement = connection.prepareStatement(DELETE_AND_RETURN_HASH_SQL)) {
            preparedStatement.setObject(1, streamErrorId);
            try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getString("hash");
                } else {
                    // hash is non-nullable so this should never happen
                    throw new StreamErrorHandlingException(format("No hash found for id '%s'", streamErrorId));
                }
            }
        }
    }

    public boolean noErrorsExistFor(final String hash, final Connection connection) throws SQLException {

        try(final PreparedStatement preparedStatement = connection.prepareStatement(ERRORS_EXIST_FOR_HASH_SQL)) {
            preparedStatement.setString(1, hash);
            try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                return ! resultSet.next();
            }
        }
    }
}
