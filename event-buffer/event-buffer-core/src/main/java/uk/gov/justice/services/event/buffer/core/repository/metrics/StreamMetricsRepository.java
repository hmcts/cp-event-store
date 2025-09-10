package uk.gov.justice.services.event.buffer.core.repository.metrics;

import static java.util.Optional.empty;
import static java.util.Optional.of;

import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Optional;

import javax.inject.Inject;

import org.slf4j.Logger;

public class StreamMetricsRepository {

    public static final String LOCK_TABLE_STREAM_STATISTIC_IN_EXCLUSIVE_MODE_NOWAIT = """
            LOCK TABLE stream_statistic IN EXCLUSIVE MODE NOWAIT
            """;

    private static final String STREAM_METRICS_SQL = """
            SELECT
                total_count,
                blocked_count,
                unblocked_count,
                stale_count,
                fresh_count
            FROM stream_statistic
            WHERE source = ?
            AND component = ?;
            """;

    private static final String CALCULATE_STREAM_STATISTIC_SQL = """
                INSERT INTO stream_statistic (source, component, total_count, blocked_count, unblocked_count, stale_count, fresh_count)
                SELECT
                        source,
                        component,
                        COUNT(*) AS total_count,
                        COUNT(*) FILTER (WHERE stream_error_id IS NOT NULL) AS blocked_count,
                        COUNT(*) FILTER (WHERE stream_error_id IS NULL) AS unblocked_count,
                        COUNT(*) FILTER (WHERE NOT is_up_to_date) AS stale_count,
                        COUNT(*) FILTER (WHERE is_up_to_date) AS fresh_count
                    FROM stream_status
                    GROUP BY source, component
                ON CONFLICT (source, component) DO UPDATE SET
                    total_count = EXCLUDED.total_count,
                    blocked_count = EXCLUDED.blocked_count,
                    unblocked_count = EXCLUDED.unblocked_count,
                    stale_count = EXCLUDED.stale_count,
                    fresh_count = EXCLUDED.fresh_count,
                    updated_at = CURRENT_TIMESTAMP
            """;

    private static final String MOST_RECENT_UPDATED_AT_SQL =
            "SELECT MAX(updated_at) as most_recent_updated_at FROM stream_statistic";

    @Inject
    private Logger logger;

    @Inject
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    public Optional<StreamMetrics> getStreamMetrics(final String source, final String component) {

        try (final Connection connection = viewStoreJdbcDataSourceProvider.getDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(STREAM_METRICS_SQL)) {
             preparedStatement.setString(1, source);
             preparedStatement.setString(2, component);

             try(final ResultSet resultSet = preparedStatement.executeQuery()){

                if (resultSet.next()) {
                    final int streamCount = resultSet.getInt("total_count");
                    final int upToDateStreamsCount = resultSet.getInt("fresh_count");
                    final int outOfDateStreamsCount = resultSet.getInt("stale_count");
                    final int blockedStreamsCount = resultSet.getInt("blocked_count");
                    final int unblockedStreamsCount = resultSet.getInt("unblocked_count");

                    final StreamMetrics streamMetrics = new StreamMetrics(
                            source,
                            component,
                            streamCount,
                            upToDateStreamsCount,
                            outOfDateStreamsCount,
                            blockedStreamsCount,
                            unblockedStreamsCount
                    );

                    return of(streamMetrics);
                }

                return empty();
            }

        } catch (final SQLException e) {
            throw new MetricsJdbcException("Failed to get metrics from stream_status table", e);
        }
    }

    public void calculateStreamStatistic(final Timestamp freshnessLimit) {
        try (final Connection connection = viewStoreJdbcDataSourceProvider.getDataSource().getConnection()) {
            if (!isUpdateNeeded(connection, freshnessLimit)) {
                return;
            }

            try (final PreparedStatement streamStatisticsLockStatement = connection.prepareStatement(LOCK_TABLE_STREAM_STATISTIC_IN_EXCLUSIVE_MODE_NOWAIT)) {
                streamStatisticsLockStatement.executeUpdate();
            } catch (final SQLException e) {
                logger.info("Failed to acquire the lock for stream statistics", e);
                return;
            }

            try (final PreparedStatement streamStatisticsStatement = connection.prepareStatement(CALCULATE_STREAM_STATISTIC_SQL)) {
                streamStatisticsStatement.executeUpdate();
            }
        } catch (final SQLException e) {
            throw new MetricsJdbcException("Failed to update stream_statistic table", e);
        }

    }

    private boolean isUpdateNeeded(final Connection connection, final Timestamp freshnessLimit) throws SQLException {
        try (final PreparedStatement checkMostRecentStatement = connection.prepareStatement(MOST_RECENT_UPDATED_AT_SQL);
             final ResultSet resultSet = checkMostRecentStatement.executeQuery()) {

            if (resultSet.next()) {
                final Timestamp mostRecentUpdatedAt = resultSet.getTimestamp("most_recent_updated_at");
                return mostRecentUpdatedAt == null || mostRecentUpdatedAt.before(freshnessLimit);
            } else {
                return true;
            }
        }
    }
}
