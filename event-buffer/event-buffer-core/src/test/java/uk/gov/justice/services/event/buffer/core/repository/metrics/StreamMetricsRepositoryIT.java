package uk.gov.justice.services.event.buffer.core.repository.metrics;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;
import uk.gov.justice.services.test.utils.persistence.TestJdbcDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Optional;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StreamMetricsRepositoryIT {

    @Mock
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    @InjectMocks
    private StreamMetricsRepository streamMetricsRepository;

    private final DatabaseCleaner databaseCleaner = new DatabaseCleaner();

    @BeforeEach
    public void cleanTables() {
        databaseCleaner.cleanViewStoreTables("framework", "stream_statistic");
    }

    @Test
    public void shouldGetMetricsBySourceAndComponent() throws Exception {

        final DataSource viewStoreDataSource = new TestJdbcDataSourceProvider().getViewStoreDataSource("framework");
        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);

        final String source = "some-source";
        final String component = "some-component";
        final int totalCount = 23;
        final int blockedCount = 876;
        final int unblockedCount = 234;
        final int staleCount = 24;
        final int freshCount = 2525;

        insertData(
                viewStoreDataSource,
                source,
                component,
                totalCount,
                blockedCount,
                unblockedCount,
                staleCount,
                freshCount);

        final Optional<StreamMetrics> streamMetrics = streamMetricsRepository.getStreamMetrics(source, component);

        if (streamMetrics.isPresent()) {
            assertThat(streamMetrics.get().source(), is(source));
            assertThat(streamMetrics.get().component(), is(component));
            assertThat(streamMetrics.get().streamCount(), is(totalCount));
            assertThat(streamMetrics.get().blockedStreamCount(), is(blockedCount));
            assertThat(streamMetrics.get().unblockedStreamCount(), is(unblockedCount));
            assertThat(streamMetrics.get().outOfDateStreamCount(), is(staleCount));
            assertThat(streamMetrics.get().upToDateStreamCount(), is(freshCount));
        } else {
            fail();
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void insertData(
            final DataSource viewStoreDataSource,
            final String source,
            final String component,
            final int totalCount,
            final int blockedCount,
            final int unblockedCount,
            final int staleCount,
            final int freshCount) throws SQLException {
        final String insertSql = """
                INSERT INTO stream_statistic(
                    source,
                    component,
                    total_count,
                    blocked_count,
                    unblocked_count,
                    stale_count,
                    fresh_count)
                VALUES(?, ?, ?, ?, ?, ?, ?)
                """;

        try (final Connection connection = viewStoreDataSource.getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(insertSql)) {

            preparedStatement.setString(1, source);
            preparedStatement.setString(2, component);
            preparedStatement.setInt(3, totalCount);
            preparedStatement.setInt(4, blockedCount);
            preparedStatement.setInt(5, unblockedCount);
            preparedStatement.setInt(6, staleCount);
            preparedStatement.setInt(7, freshCount);

            preparedStatement.execute();
        }
    }
}