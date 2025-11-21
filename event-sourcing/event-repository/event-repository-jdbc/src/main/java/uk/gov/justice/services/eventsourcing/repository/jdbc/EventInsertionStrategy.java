package uk.gov.justice.services.eventsourcing.repository.jdbc;

import static java.lang.String.format;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.toSqlTimestamp;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.Event;
import uk.gov.justice.services.eventsourcing.repository.jdbc.exception.InvalidPositionException;
import uk.gov.justice.services.eventsourcing.repository.jdbc.exception.OptimisticLockingRetryException;
import uk.gov.justice.services.jdbc.persistence.PreparedStatementWrapper;

import java.sql.SQLException;

public class EventInsertionStrategy {

    // 'is_published' is true by default so setting false to allow the publishing to set to true
    private static final String INSERT_EVENT_INTO_EVENT_LOG_SQL = """
            INSERT INTO
            event_log (
                 id,
                 stream_id,
                 position_in_stream,
                 name,
                 metadata,
                 payload,
                 date_created,
                 event_number,
                 previous_event_number,
                 is_published)
            VALUES(?, ?, ?, ?, ?, ?, ?, NULL, NULL, FALSE)
            ON CONFLICT DO NOTHING
            """;

    public String insertStatement() {
        return INSERT_EVENT_INTO_EVENT_LOG_SQL;
    }

    public void insert(final PreparedStatementWrapper preparedStatementWrapper, final Event event) throws SQLException, InvalidPositionException {
        if (event.getPositionInStream() == null) {
            throw new InvalidPositionException(format(
                    "Failed to insert event into event log table. Event has NULL positionInStream: event id '%s', streamId '%s'",
                    event.getId(),
                    event.getStreamId()));
        }

        preparedStatementWrapper.setObject(1, event.getId());
        preparedStatementWrapper.setObject(2, event.getStreamId());
        preparedStatementWrapper.setLong(3, event.getPositionInStream());
        preparedStatementWrapper.setString(4, event.getName());
        preparedStatementWrapper.setString(5, event.getMetadata());
        preparedStatementWrapper.setString(6, event.getPayload());
        preparedStatementWrapper.setTimestamp(7, toSqlTimestamp(event.getCreatedAt()));
        final int updatedRows = preparedStatementWrapper.executeUpdate();

        if (updatedRows == 0) {
            throw new OptimisticLockingRetryException(
                    format("%s while storing positionInStream '%d' of stream '%s'",
                            OptimisticLockingRetryException.class.getSimpleName(),
                            event.getPositionInStream(),
                            event.getStreamId()));
        }
    }
}
