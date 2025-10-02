package uk.gov.justice.services.eventsourcing.repository.jdbc;

import static java.lang.String.format;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.toSqlTimestamp;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.Event;
import uk.gov.justice.services.eventsourcing.repository.jdbc.exception.InvalidPositionException;
import uk.gov.justice.services.eventsourcing.repository.jdbc.exception.OptimisticLockingRetryException;
import uk.gov.justice.services.jdbc.persistence.PreparedStatementWrapper;

import java.sql.SQLException;

public class EventInsertionStrategy {

    static final String SQL_INSERT_EVENT = "INSERT INTO event_log (id, stream_id, position_in_stream, name, metadata, payload, date_created) " +
                                           "VALUES(?, ?, ?, ?, ?, ?, ?)  ON CONFLICT DO NOTHING";


    public String insertStatement() {
        return SQL_INSERT_EVENT ;
    }

    public void insert(final PreparedStatementWrapper ps, final Event event) throws SQLException, InvalidPositionException {
        final int updatedRows = executeStatement(ps, event);

        if (updatedRows == 0) {
            throw new OptimisticLockingRetryException(format("Locking Exception while storing sequence %s of stream %s",
                    event.getPositionInStream(), event.getStreamId()));
        }
    }

    private int executeStatement(final PreparedStatementWrapper preparedStatement, final Event event) throws SQLException, InvalidPositionException {
        if (event.getPositionInStream() == null) {
            throw new InvalidPositionException(format("Version is null for stream %s", event.getStreamId()));
        }

        preparedStatement.setObject(1, event.getId());
        preparedStatement.setObject(2, event.getStreamId());
        preparedStatement.setLong(3, event.getPositionInStream());
        preparedStatement.setString(4, event.getName());
        preparedStatement.setString(5, event.getMetadata());
        preparedStatement.setString(6, event.getPayload());
        preparedStatement.setTimestamp(7, toSqlTimestamp(event.getCreatedAt()));
        return preparedStatement.executeUpdate();
    }

}
