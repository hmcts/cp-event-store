package uk.gov.justice.services.resources.repository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.inject.Inject;
import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRowMapper;
import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamStatus;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;

import static java.lang.String.format;
import static java.util.Optional.ofNullable;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.fromSqlTimestamp;

@SuppressWarnings("java:S1192")
public class StreamStatusReadRepository {
    private static final String FIND_BY_ERROR_HASH = """
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
                FROM stream_status WHERE stream_id in (select distinct stream_id from stream_error where hash = ?)
                ORDER BY updated_at DESC
            """;

    @Inject
    private NewStreamStatusRowMapper streamStatusRowMapper;

    @Inject
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    public List<StreamStatus> findBy(final String errorHash) {
        final List<StreamStatus> streamStatuses = new ArrayList<>();
        try (final Connection connection = viewStoreJdbcDataSourceProvider.getDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(FIND_BY_ERROR_HASH)) {

            preparedStatement.setObject(1, errorHash);

            try (final ResultSet resultSet = preparedStatement.executeQuery()) {

                while(resultSet.next()) {
                    final StreamStatus streamStatus = streamStatusRowMapper.mapRow(resultSet);
                    streamStatuses.add(streamStatus);
                }
            }

            return streamStatuses;

        } catch (final SQLException e) {
            throw new StreamQueryException(format(
                    "Failed to query streams by error hash '%s",
                    errorHash), e);
        }
    }
}
