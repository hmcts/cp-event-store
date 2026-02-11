package uk.gov.justice.services.event.buffer.core.repository.streamerror;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.inject.Inject;
import javax.sql.DataSource;

public class StreamErrorRetryRepository {

    static final String UPSERT_SQL = """
            INSERT INTO stream_error_retry (
                stream_id,
                source,
                component,
                updated_at,
                retry_count,
                next_retry_time
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (stream_id, source, component) DO UPDATE
            SET
                retry_count = Excluded.retry_count,
                updated_at = Excluded.updated_at,
                next_retry_time = Excluded.next_retry_time
            """;

    static final String FIND_BY_SQL = """
            SELECT
                updated_at,
                retry_count,
                next_retry_time
            FROM stream_error_retry
            WHERE stream_id = ?
            AND source = ?
            AND component = ?
            """;

    static final String FIND_ALL_SQL = """
            SELECT
                stream_id,
                source,
                component,
                updated_at,
                retry_count,
                next_retry_time
            FROM stream_error_retry
            """;

    @Inject
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    public void upsert(final StreamErrorRetry streamErrorRetry) {
        final DataSource viewStoreDataSource = viewStoreJdbcDataSourceProvider.getDataSource();

        try (final Connection connection = viewStoreDataSource.getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(UPSERT_SQL)) {

            preparedStatement.setObject(1, streamErrorRetry.streamId());
            preparedStatement.setString(2, streamErrorRetry.source());
            preparedStatement.setString(3, streamErrorRetry.component());
            preparedStatement.setTimestamp(4, toSqlTimestamp(streamErrorRetry.occurredAt()));
            preparedStatement.setLong(5, streamErrorRetry.retryCount());
            preparedStatement.setTimestamp(6, toSqlTimestamp(streamErrorRetry.nextRetryTime()));

            preparedStatement.executeUpdate();

        } catch (final SQLException e) {
            throw new StreamErrorPersistenceException(format("Failed to upsert %s", streamErrorRetry), e);
        }
    }

    public Optional<StreamErrorRetry> findBy(
            final UUID streamId,
            final String source,
            final String component) {

        final DataSource viewStoreDataSource = viewStoreJdbcDataSourceProvider.getDataSource();

        try (final Connection connection = viewStoreDataSource.getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(FIND_BY_SQL)) {

            preparedStatement.setObject(1, streamId);
            preparedStatement.setString(2, source);
            preparedStatement.setString(3, component);

            try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    final ZonedDateTime updatedAt = fromSqlTimestamp(resultSet.getTimestamp("updated_at"));
                    final long retryCount = resultSet.getLong("retry_count");
                    final ZonedDateTime nextRetryTime = fromSqlTimestamp(resultSet.getTimestamp("next_retry_time"));

                    return of(new StreamErrorRetry(
                            streamId,
                            source,
                            component,
                            updatedAt,
                            retryCount,
                            nextRetryTime));
                }
            }
        } catch (final SQLException e) {
            throw new StreamErrorPersistenceException(
                    format("Failed to find StreamErrorRetry by streamId: '%s', source: '%s', component: '%s'",
                            streamId,
                            source,
                            component),
                    e);
        }

        return empty();
    }

    public List<StreamErrorRetry> findAll() {

        final DataSource viewStoreDataSource = viewStoreJdbcDataSourceProvider.getDataSource();

        try (final Connection connection = viewStoreDataSource.getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(FIND_ALL_SQL);
             final ResultSet resultSet = preparedStatement.executeQuery()) {

            final List<StreamErrorRetry> streamErrorRetries = new ArrayList<>();

            while (resultSet.next()) {
                final UUID streamId = resultSet.getObject("stream_id", UUID.class);
                final String source = resultSet.getString("source");
                final String component = resultSet.getString("component");
                final ZonedDateTime updatedAt = fromSqlTimestamp(resultSet.getTimestamp("updated_at"));
                final long retryCount = resultSet.getLong("retry_count");
                final ZonedDateTime nextRetryTime = fromSqlTimestamp(resultSet.getTimestamp("next_retry_time"));

                final StreamErrorRetry streamErrorRetry = new StreamErrorRetry(
                        streamId,
                        source,
                        component,
                        updatedAt,
                        retryCount,
                        nextRetryTime);

                streamErrorRetries.add(streamErrorRetry);
            }

            return unmodifiableList(streamErrorRetries);

        } catch (final SQLException e) {
            throw new StreamErrorPersistenceException("Failed to find all StreamErrorRetries", e);
        }
    }
}
