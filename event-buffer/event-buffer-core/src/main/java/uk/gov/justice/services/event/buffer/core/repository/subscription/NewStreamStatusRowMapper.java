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
        final Long position = resultSet.getObject("position", Long.class);
        final Optional<UUID> streamErrorId = ofNullable((UUID) resultSet.getObject("stream_error_id"));
        final Optional<Long> streamErrorPosition = ofNullable(resultSet.getObject("stream_error_position", Long.class));
        final ZonedDateTime discoveredAt = fromSqlTimestamp(resultSet.getTimestamp("discovered_at"));
        final Long latestKnownPosition = resultSet.getObject("latest_known_position", Long.class);
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
                discoveredAt,
                latestKnownPosition,
                isUpToDate
        );
    }
}
