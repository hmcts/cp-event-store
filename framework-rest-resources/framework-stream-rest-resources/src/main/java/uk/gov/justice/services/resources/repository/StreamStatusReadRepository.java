package uk.gov.justice.services.resources.repository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.inject.Inject;
import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRowMapper;
import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamStatus;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;

import static java.lang.String.format;

@SuppressWarnings("java:S1192")
public class StreamStatusReadRepository {

    private static final String SELECT_CLAUSE = """
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
            """;
    private static final String FIND_BY_ERROR_HASH = """
                %s 
                 FROM stream_status WHERE stream_id in (select stream_id from stream_error where hash = ?)
                ORDER BY updated_at DESC
            """.formatted(SELECT_CLAUSE);

    private static final String FIND_BY_STREAM_ID = """
                %s 
                 FROM stream_status WHERE stream_id = ?
                ORDER BY updated_at DESC
            """.formatted(SELECT_CLAUSE);

    private static final String FIND_ERROR_STREAMS = """
                %s 
                 FROM stream_status WHERE stream_error_id IS NOT NULL
                ORDER BY updated_at DESC
            """.formatted(SELECT_CLAUSE);

    @Inject
    private NewStreamStatusRowMapper streamStatusRowMapper;

    @Inject
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    public List<StreamStatus> findByErrorHash(final String errorHash) {
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

    public List<StreamStatus> findByStreamId(final UUID streamId) {
        final List<StreamStatus> streamStatuses = new ArrayList<>();
        try (final Connection connection = viewStoreJdbcDataSourceProvider.getDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(FIND_BY_STREAM_ID)) {

            preparedStatement.setObject(1, streamId);

            try (final ResultSet resultSet = preparedStatement.executeQuery()) {

                while(resultSet.next()) {
                    final StreamStatus streamStatus = streamStatusRowMapper.mapRow(resultSet);
                    streamStatuses.add(streamStatus);
                }
            }

            return streamStatuses;

        } catch (final SQLException e) {
            throw new StreamQueryException(format(
                    "Failed to query streams by streamId '%s",
                    streamId), e);
        }
    }

    public List<StreamStatus> findErrorStreams() {
        final List<StreamStatus> streamStatuses = new ArrayList<>();
        try (final Connection connection = viewStoreJdbcDataSourceProvider.getDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(FIND_ERROR_STREAMS)) {

            try (final ResultSet resultSet = preparedStatement.executeQuery()) {

                while(resultSet.next()) {
                    final StreamStatus streamStatus = streamStatusRowMapper.mapRow(resultSet);
                    streamStatuses.add(streamStatus);
                }
            }

            return streamStatuses;

        } catch (final SQLException e) {
            throw new StreamQueryException(format(
                    "Failed to query errored streams"), e);
        }
    }
}
