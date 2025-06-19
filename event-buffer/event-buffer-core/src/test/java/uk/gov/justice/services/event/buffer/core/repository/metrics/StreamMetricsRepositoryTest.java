package uk.gov.justice.services.event.buffer.core.repository.metrics;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.core.annotation.Component.EVENT_LISTENER;
import static uk.gov.justice.services.core.annotation.Component.EVENT_PROCESSOR;

import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;

import javax.sql.DataSource;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StreamMetricsRepositoryTest {

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

    private static final String CALCULATE_STREAM_STATISTIC = """
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

    @Mock
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    @InjectMocks
    private StreamMetricsRepository streamMetricsRepository;

    @Test
    public void shouldGetMetricsFromTheStreamStatusTable() throws Exception {

        final StreamMetrics eventListenerStreamMetrics = new StreamMetrics(
                "hearing",
                EVENT_LISTENER, 234, 329, 23, 87, 78);
        final StreamMetrics eventProcessorStreamMetrics = new StreamMetrics(
                "hearing",
                EVENT_PROCESSOR, 23, 64, 25, 477, 34);

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(STREAM_METRICS_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, true, false);

        when(resultSet.getString("source")).thenReturn(eventListenerStreamMetrics.source(), eventProcessorStreamMetrics.source());
        when(resultSet.getString("component")).thenReturn(eventListenerStreamMetrics.component(), eventProcessorStreamMetrics.component());
        when(resultSet.getInt("stream_count")).thenReturn(eventListenerStreamMetrics.streamCount(), eventProcessorStreamMetrics.streamCount());
        when(resultSet.getInt("up_to_date_streams_count")).thenReturn(eventListenerStreamMetrics.upToDateStreamCount(), eventProcessorStreamMetrics.upToDateStreamCount());
        when(resultSet.getInt("out_of_date_streams_count")).thenReturn(eventListenerStreamMetrics.outOfDateStreamCount(), eventProcessorStreamMetrics.outOfDateStreamCount());
        when(resultSet.getInt("blocked_streams_count")).thenReturn(eventListenerStreamMetrics.blockedStreamCount(), eventProcessorStreamMetrics.blockedStreamCount());
        when(resultSet.getInt("unblocked_streams_count")).thenReturn(eventListenerStreamMetrics.unblockedStreamCount(), eventProcessorStreamMetrics.unblockedStreamCount());

        final List<StreamMetrics> streamMetrics = streamMetricsRepository.getStreamMetrics();
        assertThat(streamMetrics.size(), is(2));
        assertThat(streamMetrics.get(0), is(eventListenerStreamMetrics));
        assertThat(streamMetrics.get(1), is(eventProcessorStreamMetrics));

        verify(connection).close();
        verify(preparedStatement).close();
        verify(resultSet).close();
    }

    @Test
    public void shouldThrowMetricsJdbcExceptionIfDatabaseAccessFails() throws Exception {

        final StreamMetrics eventListenerStreamMetrics = new StreamMetrics(
                "hearing",
                EVENT_LISTENER, 234, 329, 23, 87, 78);
        final StreamMetrics eventProcessorStreamMetrics = new StreamMetrics(
                "hearing",
                EVENT_PROCESSOR, 23, 64, 25, 477, 34);
        final SQLException sqlException = new SQLException("Ooops");

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(STREAM_METRICS_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenThrow(sqlException);

        final MetricsJdbcException metricsJdbcException = assertThrows(MetricsJdbcException.class, () -> streamMetricsRepository.getStreamMetrics());
        assertThat(metricsJdbcException.getCause(), is(sqlException));
        assertThat(metricsJdbcException.getMessage(), is("Failed to get metrics from stream_status table"));

        verify(connection).close();
        verify(preparedStatement).close();
        verify(resultSet).close();
    }

    @Test
    public void shouldCalculateStreamStatisticWhenDataIsNotFresh() throws Exception {

        final Timestamp freshnessLimit = Timestamp.valueOf(LocalDateTime.now());

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement checkMostRecentStatement = mock(PreparedStatement.class);
        final PreparedStatement mergedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(MOST_RECENT_UPDATED_AT_SQL)).thenReturn(checkMostRecentStatement);
        when(connection.prepareStatement(CALCULATE_STREAM_STATISTIC)).thenReturn(mergedStatement);
        when(checkMostRecentStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getTimestamp("most_recent_updated_at")).thenReturn(Timestamp.valueOf(LocalDateTime.now().minusHours(1)));

        // run
        streamMetricsRepository.calculateStreamStatistic(freshnessLimit);

        // verify
        verify(mergedStatement).executeUpdate();
        verify(connection).close();
        verify(checkMostRecentStatement).close();
        verify(mergedStatement).close();
        verify(resultSet).close();
    }

    @Test
    public void shouldNotCalculateStreamStatisticWhenDataIsFresh() throws Exception {
        final Timestamp freshnessLimit = Timestamp.valueOf(LocalDateTime.now().minusHours(1));

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement checkMostRecentStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(MOST_RECENT_UPDATED_AT_SQL)).thenReturn(checkMostRecentStatement);
        when(checkMostRecentStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getTimestamp("most_recent_updated_at")).thenReturn(Timestamp.valueOf(LocalDateTime.now()));

        // run
        streamMetricsRepository.calculateStreamStatistic(freshnessLimit);

        // verify
        verify(connection, never()).prepareStatement(CALCULATE_STREAM_STATISTIC);
        verify(connection).close();
        verify(checkMostRecentStatement).close();
        verify(resultSet).close();
    }

    @Test
    public void shouldCalculateStreamStatisticWhenNoDataExist() throws Exception {
        final Timestamp freshnessLimit = Timestamp.valueOf(LocalDateTime.now());

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement checkMostRecentStatement = mock(PreparedStatement.class);
        final PreparedStatement mergedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(MOST_RECENT_UPDATED_AT_SQL)).thenReturn(checkMostRecentStatement);
        when(connection.prepareStatement(CALCULATE_STREAM_STATISTIC)).thenReturn(mergedStatement);
        when(checkMostRecentStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getTimestamp("most_recent_updated_at")).thenReturn(null); // No updates exist

        // Run
        streamMetricsRepository.calculateStreamStatistic(freshnessLimit);

        // verify
        verify(mergedStatement).executeUpdate();
        verify(connection).close();
        verify(checkMostRecentStatement).close();
        verify(mergedStatement).close();
        verify(resultSet).close();
    }

    @Test
    public void shouldThrowMetricsJdbcExceptionIfDatabaseAccessFailsWhenCalculatingStreamStatistic() throws Exception {
        final Timestamp freshnessLimit = Timestamp.valueOf(LocalDateTime.now());
        final SQLException sqlException = new SQLException("Some locking exception");

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(MOST_RECENT_UPDATED_AT_SQL)).thenThrow(sqlException);

        // run
        final MetricsJdbcException metricsJdbcException = assertThrows(MetricsJdbcException.class,
                () -> streamMetricsRepository.calculateStreamStatistic(freshnessLimit));

        // verify
        assertThat(metricsJdbcException.getCause(), is(sqlException));
        assertThat(metricsJdbcException.getMessage(), is("Failed to update stream_statistic table"));

        verify(connection).close();
    }
}
