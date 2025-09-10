package uk.gov.justice.services.event.buffer.core.repository.streamerror;

import static java.util.Optional.ofNullable;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.fromSqlTimestamp;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.UUID;

public class StreamErrorDetailsRowMapper {

    public StreamErrorDetails mapRow(ResultSet resultSet) throws SQLException {
        final UUID id = (UUID) resultSet.getObject("id");
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

        return new StreamErrorDetails(
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
                source
        );
    }
}
