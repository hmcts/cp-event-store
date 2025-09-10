package uk.gov.justice.services.event.buffer.core.repository.subscription;

import static java.util.Optional.ofNullable;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.fromSqlTimestamp;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;

public class NewStreamStatusRowMapper {

    public StreamStatus mapRow(ResultSet resultSet) throws SQLException {
        final Long position = resultSet.getLong("position");
        final Optional<UUID> streamErrorId = ofNullable((UUID) resultSet.getObject("stream_error_id"));
        final Optional<Long> streamErrorPosition = ofNullable((Long) resultSet.getObject("stream_error_position"));
        final ZonedDateTime updatedAt = fromSqlTimestamp(resultSet.getTimestamp("updated_at"));
        final Long latestKnownPosition = resultSet.getLong("latest_known_position");
        final Boolean isUpToDate = resultSet.getBoolean("is_up_to_date");
        final String source = resultSet.getString("source");
        final String componentName = resultSet.getString("component");
        final UUID streamId = (UUID) resultSet.getObject("stream_id");

        return new StreamStatus(
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
    }
}
