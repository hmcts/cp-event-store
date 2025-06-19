package uk.gov.justice.services.event.buffer.core.repository.metrics;

import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

public class StreamMetricsRepository {

    private static final String STREAM_METRICS_SQL = """
            SELECT
                    source,
                    component,
                    COUNT(*)                                              AS stream_count,
                    COUNT(*) FILTER (WHERE is_up_to_date)                 AS up_to_date_streams_count,
                    COUNT(*) FILTER (WHERE NOT is_up_to_date)             AS out_of_date_streams_count,
                    COUNT(*) FILTER (WHERE stream_error_id IS NOT NULL)   AS blocked_streams_count,
                    COUNT(*) FILTER (WHERE stream_error_id IS NULL)       AS unblocked_streams_count
                FROM stream_status
                GROUP BY source, component;
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
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    public List<StreamMetrics> getStreamMetrics() {

        try (final Connection connection = viewStoreJdbcDataSourceProvider.getDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(STREAM_METRICS_SQL);
             final ResultSet resultSet = preparedStatement.executeQuery()) {

            final ArrayList<StreamMetrics> streamMetrics = new ArrayList<>();

            while (resultSet.next()) {
                final String source = resultSet.getString("source");
                final String component = resultSet.getString("component");
                final int streamCount = resultSet.getInt("stream_count");
                final int upToDateStreamsCount = resultSet.getInt("up_to_date_streams_count");
                final int outOfDateStreamsCount = resultSet.getInt("out_of_date_streams_count");
                final int blockedStreamsCount = resultSet.getInt("blocked_streams_count");
                final int unblockedStreamsCount = resultSet.getInt("unblocked_streams_count");

                streamMetrics.add(new StreamMetrics(
                        source,
                        component,
                        streamCount,
                        upToDateStreamsCount,
                        outOfDateStreamsCount,
                        blockedStreamsCount,
                        unblockedStreamsCount
                ));
            }

            return streamMetrics;

        } catch (final SQLException e) {
            throw new MetricsJdbcException("Failed to get metrics from stream_status table", e);
        }
    }

    public void calculateStreamStatistic(Timestamp freshnessLimit) {
        try (final Connection connection = viewStoreJdbcDataSourceProvider.getDataSource().getConnection()) {
            if (!isUpdateNeeded(connection, freshnessLimit)) {
                return;
            }

            try (final PreparedStatement streamStatisticsStatement = connection.prepareStatement(CALCULATE_STREAM_STATISTIC_SQL)) {
                streamStatisticsStatement.executeUpdate();
            }
        } catch (final SQLException e) {
            throw new MetricsJdbcException("Failed to update stream_statistic table", e);
        }

    }

    private boolean isUpdateNeeded(Connection connection, Timestamp freshnessLimit) throws SQLException {
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
