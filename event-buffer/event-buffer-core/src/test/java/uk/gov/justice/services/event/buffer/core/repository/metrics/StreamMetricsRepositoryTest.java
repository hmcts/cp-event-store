package uk.gov.justice.services.event.buffer.core.repository.metrics;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Optional;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class StreamMetricsRepositoryTest {

    private static final String LOCK_TABLE_STREAM_STATISTIC_IN_EXCLUSIVE_MODE_NOWAIT = """
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

    @Mock
    private Logger logger;

    @InjectMocks
    private StreamMetricsRepository streamMetricsRepository;

    @Test
    public void shouldGetMetricsFromTheStreamStatusTable() throws Exception {

        final String source = "some-source";
        final String component = "some-component";
        final int streamCount = 23;
        final int upToDateStreamCount = 345;
        final int outOfDateStreamCount = 9782;
        final int blockedStreamCount = 252;
        final int unblockedStreamCount = 727;

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(STREAM_METRICS_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);

        when(resultSet.getInt("total_count")).thenReturn(streamCount);
        when(resultSet.getInt("fresh_count")).thenReturn(upToDateStreamCount);
        when(resultSet.getInt("stale_count")).thenReturn(outOfDateStreamCount);
        when(resultSet.getInt("blocked_count")).thenReturn(blockedStreamCount);
        when(resultSet.getInt("unblocked_count")).thenReturn(unblockedStreamCount);

        final Optional<StreamMetrics> streamMetrics = streamMetricsRepository.getStreamMetrics(source, component);

        if (streamMetrics.isPresent()) {
            assertThat(streamMetrics.get().source(), is(source));
            assertThat(streamMetrics.get().component(), is(component));
            assertThat(streamMetrics.get().streamCount(), is(streamCount));
            assertThat(streamMetrics.get().upToDateStreamCount(), is(upToDateStreamCount));
            assertThat(streamMetrics.get().outOfDateStreamCount(), is(outOfDateStreamCount));
            assertThat(streamMetrics.get().blockedStreamCount(), is(blockedStreamCount));
            assertThat(streamMetrics.get().unblockedStreamCount(), is(unblockedStreamCount));
        } else {
            fail();
        }

        final InOrder inOrder = inOrder(resultSet, preparedStatement, connection);
        inOrder.verify(preparedStatement).setString(1, source);
        inOrder.verify(preparedStatement).setString(2, component);
        inOrder.verify(resultSet).close();
        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldThrowMetricsJdbcExceptionIfDatabaseAccessFails() throws Exception {

        final SQLException sqlException = new SQLException("Ooops");
        final String source = "some-source";
        final String component = "some-component";
        final int streamCount = 23;
        final int upToDateStreamCount = 345;
        final int outOfDateStreamCount = 9782;
        final int blockedStreamCount = 252;

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(STREAM_METRICS_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);

        when(resultSet.getInt("total_count")).thenReturn(streamCount);
        when(resultSet.getInt("fresh_count")).thenReturn(upToDateStreamCount);
        when(resultSet.getInt("stale_count")).thenReturn(outOfDateStreamCount);
        when(resultSet.getInt("blocked_count")).thenReturn(blockedStreamCount);
        when(resultSet.getInt("unblocked_count")).thenThrow(sqlException);

        final MetricsJdbcException metricsJdbcException = assertThrows(
                MetricsJdbcException.class,
                () -> streamMetricsRepository.getStreamMetrics(source, component));

        assertThat(metricsJdbcException.getCause(), is(sqlException));
        assertThat(metricsJdbcException.getMessage(), is("Failed to get metrics from stream_status table"));

        verify(resultSet).close();
    }

    @Test
    public void shouldCalculateStreamStatisticWhenDataIsNotFresh() throws Exception {

        final Timestamp freshnessLimit = Timestamp.valueOf(LocalDateTime.now());

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement checkMostRecentStatement = mock(PreparedStatement.class);
        final PreparedStatement streamStatisticsLockStatement = mock(PreparedStatement.class);
        final PreparedStatement mergedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(MOST_RECENT_UPDATED_AT_SQL)).thenReturn(checkMostRecentStatement);
        when(connection.prepareStatement(LOCK_TABLE_STREAM_STATISTIC_IN_EXCLUSIVE_MODE_NOWAIT)).thenReturn(streamStatisticsLockStatement);
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
    public void shouldNotExecuteCalculationIfLockingStreamStatisticTableFails() throws Exception {

        final Timestamp freshnessLimit = Timestamp.valueOf(LocalDateTime.now());
        final SQLException sqlException = new SQLException("Some locking exception");

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement checkMostRecentStatement = mock(PreparedStatement.class);
        final PreparedStatement streamStatisticsLockStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(LOCK_TABLE_STREAM_STATISTIC_IN_EXCLUSIVE_MODE_NOWAIT)).thenReturn(streamStatisticsLockStatement);
        when(connection.prepareStatement(MOST_RECENT_UPDATED_AT_SQL)).thenReturn(checkMostRecentStatement);
        when(streamStatisticsLockStatement.executeUpdate()).thenThrow(sqlException);
        when(checkMostRecentStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getTimestamp("most_recent_updated_at")).thenReturn(Timestamp.valueOf(LocalDateTime.now().minusHours(1)));

        // run
        streamMetricsRepository.calculateStreamStatistic(freshnessLimit);

        // verify
        verify(connection, never()).prepareStatement(CALCULATE_STREAM_STATISTIC);
        verify(connection).close();
        verify(checkMostRecentStatement).close();
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
        final PreparedStatement streamStatisticsLockStatement = mock(PreparedStatement.class);
        final PreparedStatement mergedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(MOST_RECENT_UPDATED_AT_SQL)).thenReturn(checkMostRecentStatement);
        when(connection.prepareStatement(LOCK_TABLE_STREAM_STATISTIC_IN_EXCLUSIVE_MODE_NOWAIT)).thenReturn(streamStatisticsLockStatement);
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