package uk.gov.justice.services.event.buffer.core.repository.streamerror;

import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.time.ZonedDateTime.of;
import static java.util.Optional.empty;
import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.toSqlTimestamp;
import static uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorRetryRepository.DELETE_STREAM_ERROR_RETRY_SQL;
import static uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorRetryRepository.FIND_ALL_SQL;
import static uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorRetryRepository.FIND_BY_SQL;
import static uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorRetryRepository.GET_RETRY_COUNT_SQL;
import static uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorRetryRepository.UPSERT_SQL;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StreamErrorRetryRepositoryTest {

    @Mock
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    @InjectMocks
    private StreamErrorRetryRepository streamErrorRetryRepository;

    @Test
    public void shouldUpsertStreamErrorRetry() throws Exception {

        final StreamErrorRetry streamErrorRetry = new StreamErrorRetry(
                randomUUID(),
                "some-source",
                "some-component",
                new UtcClock().now(),
                677L,
                new UtcClock().now().plusMinutes(2)
        );

        final DataSource viewStoreDataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);
        when(viewStoreDataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(UPSERT_SQL)).thenReturn(preparedStatement);

        streamErrorRetryRepository.upsert(streamErrorRetry);

        final InOrder inOrder = inOrder(preparedStatement, connection);

        inOrder.verify(preparedStatement).setObject(1, streamErrorRetry.streamId());
        inOrder.verify(preparedStatement).setString(2, streamErrorRetry.source());
        inOrder.verify(preparedStatement).setString(3, streamErrorRetry.component());
        inOrder.verify(preparedStatement).setTimestamp(4, toSqlTimestamp(streamErrorRetry.occurredAt()));
        inOrder.verify(preparedStatement).setLong(5, streamErrorRetry.retryCount());
        inOrder.verify(preparedStatement).setTimestamp(6, toSqlTimestamp(streamErrorRetry.nextRetryTime()));

        inOrder.verify(preparedStatement).executeUpdate();
        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldThrowStreamErrorPersistenceExceptionIfUpsertStreamErrorRetryFails() throws Exception {

        final SQLException sqlException = new SQLException("Bunnies");
        final StreamErrorRetry streamErrorRetry = new StreamErrorRetry(
                fromString("361ed594-25b6-4d34-aac4-acf3463e9323"),
                "some-source",
                "some-component",
                of(2026, 2, 11, 12, 24, 32, 0, UTC),
                677L,
                of(2026, 2, 11, 12, 26, 32, 0, UTC)
        );

        final DataSource viewStoreDataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);


        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);
        when(viewStoreDataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(UPSERT_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenThrow(sqlException);

        final StreamErrorPersistenceException streamErrorPersistenceException = assertThrows(
                StreamErrorPersistenceException.class,
                () -> streamErrorRetryRepository.upsert(streamErrorRetry));

        assertThat(streamErrorPersistenceException.getCause(), is(sqlException));
        assertThat(streamErrorPersistenceException.getMessage(), is(format("Failed to upsert %s", streamErrorRetry)));

        verify(preparedStatement).close();
        verify(connection).close();
    }

    @Test
    public void shouldFindByStreamIdSourceAndComponent() throws Exception {

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";
        final ZonedDateTime occurredAt = new UtcClock().now();
        final Long retryCount = 6L;
        final ZonedDateTime nextRetryTime = new UtcClock().now().plusMinutes(2);

        final DataSource viewStoreDataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);
        when(viewStoreDataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(FIND_BY_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getTimestamp("updated_at")).thenReturn(toSqlTimestamp(occurredAt));
        when(resultSet.getLong("retry_count")).thenReturn(retryCount);
        when(resultSet.getTimestamp("next_retry_time")).thenReturn(toSqlTimestamp(nextRetryTime));

        final Optional<StreamErrorRetry> streamErrorRetry = streamErrorRetryRepository.findBy(streamId, source, component);

        assertThat(streamErrorRetry.isPresent(), is(true));
        assertThat(streamErrorRetry.get().streamId(), is(streamId));
        assertThat(streamErrorRetry.get().source(), is(source));
        assertThat(streamErrorRetry.get().component(), is(component));
        assertThat(streamErrorRetry.get().occurredAt(), is(occurredAt));
        assertThat(streamErrorRetry.get().retryCount(), is(retryCount));
        assertThat(streamErrorRetry.get().nextRetryTime(), is(nextRetryTime));

        verify(resultSet).close();
        verify(preparedStatement).close();
        verify(connection).close();
    }

    @Test
    public void shouldReturnEmptyIfNoStreamErrorRetryFound() throws Exception {

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";

        final DataSource viewStoreDataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);
        when(viewStoreDataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(FIND_BY_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        final Optional<StreamErrorRetry> streamErrorRetry = streamErrorRetryRepository.findBy(streamId, source, component);

        assertThat(streamErrorRetry, is(empty()));

        verify(resultSet).close();
        verify(preparedStatement).close();
        verify(connection).close();
    }

    @Test
    public void shouldThrowStreamErrorPersistenceExceptionIfFindingByStreamIdSourceAndComponentFails() throws Exception {

        final SQLException sqlException = new SQLException("Oh no");

        final UUID streamId = fromString("12b20c91-36c9-4b97-92e2-a2e76895b79f");
        final String source = "some-source";
        final String component = "some-component";

        final DataSource viewStoreDataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);
        when(viewStoreDataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(FIND_BY_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);

        when(resultSet.next()).thenThrow(sqlException);

        final StreamErrorPersistenceException streamErrorPersistenceException = assertThrows(
                StreamErrorPersistenceException.class,
                () -> streamErrorRetryRepository.findBy(streamId, source, component));

        assertThat(streamErrorPersistenceException.getCause(), is(sqlException));
        assertThat(streamErrorPersistenceException.getMessage(), is("Failed to find StreamErrorRetry by streamId: '12b20c91-36c9-4b97-92e2-a2e76895b79f', source: 'some-source', component: 'some-component'"));

        verify(resultSet).close();
        verify(preparedStatement).close();
        verify(connection).close();
    }

    @Test
    public void shouldFindAllStreamErrorRetries() throws Exception {

        final UUID streamId_1 = randomUUID();
        final String source_1 = "some-source_1";
        final String component_1 = "some-component_1";
        final ZonedDateTime occurredAt_1 = new UtcClock().now();
        final Long retryCount_1 = 1L;
        final ZonedDateTime nextRetryTime_1 = new UtcClock().now().plusMinutes(1);

        final UUID streamId_2 = randomUUID();
        final String source_2 = "some-source_2";
        final String component_2 = "some-component_2";
        final ZonedDateTime occurredAt_2 = new UtcClock().now().minusMinutes(2);
        final Long retryCount_2 = 2L;
        final ZonedDateTime nextRetryTime_2 = new UtcClock().now().plusMinutes(2);


        final DataSource viewStoreDataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);
        when(viewStoreDataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(FIND_ALL_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, true, false);

        when(resultSet.getObject("stream_id", UUID.class)).thenReturn(streamId_1, streamId_2);
        when(resultSet.getString("source")).thenReturn(source_1, source_2);
        when(resultSet.getString("component")).thenReturn(component_1, component_2);
        when(resultSet.getTimestamp("updated_at")).thenReturn(toSqlTimestamp(occurredAt_1), toSqlTimestamp(occurredAt_2));
        when(resultSet.getLong("retry_count")).thenReturn(retryCount_1, retryCount_2);
        when(resultSet.getTimestamp("next_retry_time")).thenReturn(toSqlTimestamp(nextRetryTime_1), toSqlTimestamp(nextRetryTime_2));

        final List<StreamErrorRetry> streamErrorRetries = streamErrorRetryRepository.findAll();

        assertThat(streamErrorRetries.size(), is(2));

        assertThat(streamErrorRetries.get(0).streamId(), is(streamId_1));
        assertThat(streamErrorRetries.get(0).source(), is(source_1));
        assertThat(streamErrorRetries.get(0).component(), is(component_1));
        assertThat(streamErrorRetries.get(0).occurredAt(), is(occurredAt_1));
        assertThat(streamErrorRetries.get(0).retryCount(), is(retryCount_1));
        assertThat(streamErrorRetries.get(0).nextRetryTime(), is(nextRetryTime_1));

        assertThat(streamErrorRetries.get(1).streamId(), is(streamId_2));
        assertThat(streamErrorRetries.get(1).source(), is(source_2));
        assertThat(streamErrorRetries.get(1).component(), is(component_2));
        assertThat(streamErrorRetries.get(1).occurredAt(), is(occurredAt_2));
        assertThat(streamErrorRetries.get(1).retryCount(), is(retryCount_2));
        assertThat(streamErrorRetries.get(1).nextRetryTime(), is(nextRetryTime_2));

        verify(resultSet).close();
        verify(preparedStatement).close();
        verify(connection).close();
    }

    @Test
    public void shouldThrowStreamErrorPersistenceExceptionIfFindingAllStreamErrorRetriesFails() throws Exception {

        final SQLException sqlException = new SQLException("Oh no");

        final DataSource viewStoreDataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);
        when(viewStoreDataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(FIND_ALL_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenThrow(sqlException);

        final StreamErrorPersistenceException streamErrorPersistenceException = assertThrows(
                StreamErrorPersistenceException.class,
                () -> streamErrorRetryRepository.findAll());

        assertThat(streamErrorPersistenceException.getCause(), is(sqlException));
        assertThat(streamErrorPersistenceException.getMessage(), is("Failed to find all StreamErrorRetries"));

        verify(resultSet).close();
        verify(preparedStatement).close();
        verify(connection).close();
    }

    @Test
    public void shouldGetRetryCount() throws Exception {

        final long retryCount = 23L;

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";

        final DataSource viewStoreDataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);
        when(viewStoreDataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(GET_RETRY_COUNT_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getObject("retry_count", Long.class)).thenReturn(retryCount);

        assertThat(streamErrorRetryRepository.getRetryCount(streamId, source, component), is(retryCount));

        final InOrder inOrder = inOrder(preparedStatement, resultSet, connection);

        inOrder.verify(preparedStatement).setObject(1, streamId);
        inOrder.verify(preparedStatement).setString(2, source);
        inOrder.verify(preparedStatement).setString(3, component);
        inOrder.verify(preparedStatement).executeQuery();
        inOrder.verify(resultSet).close();
        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldReturnRetryCountOfZeroIfNoStreamRetryFound() throws Exception {

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";

        final DataSource viewStoreDataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);
        when(viewStoreDataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(GET_RETRY_COUNT_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        assertThat(streamErrorRetryRepository.getRetryCount(streamId, source, component), is(0L));

        final InOrder inOrder = inOrder(preparedStatement, resultSet, connection);

        inOrder.verify(preparedStatement).setObject(1, streamId);
        inOrder.verify(preparedStatement).setString(2, source);
        inOrder.verify(preparedStatement).setString(3, component);
        inOrder.verify(preparedStatement).executeQuery();
        inOrder.verify(resultSet).close();
        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldThrowStreamErrorPersistenceExceptionIfGettingRetryCountFails() throws Exception {

        final SQLException sqlException = new SQLException("Oh no");

        final UUID streamId = fromString("6330759f-4c72-4804-bbf5-20ee024de079");
        final String source = "some-source";
        final String component = "some-component";

        final DataSource viewStoreDataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);
        when(viewStoreDataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(GET_RETRY_COUNT_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenThrow(sqlException);

        final StreamErrorPersistenceException streamErrorPersistenceException = assertThrows(
                StreamErrorPersistenceException.class,
                () -> streamErrorRetryRepository.getRetryCount(streamId, source, component));

        assertThat(streamErrorPersistenceException.getCause(), is(sqlException));
        assertThat(streamErrorPersistenceException.getMessage(), is("Failed to lookup retryCount for streamId: '6330759f-4c72-4804-bbf5-20ee024de079', source: 'some-source', component: 'some-component'"));

        final InOrder inOrder = inOrder(preparedStatement, connection);

        inOrder.verify(preparedStatement).setObject(1, streamId);
        inOrder.verify(preparedStatement).setString(2, source);
        inOrder.verify(preparedStatement).setString(3, component);
        inOrder.verify(preparedStatement).executeQuery();
        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldRemoveStreamErrorRetry() throws Exception {

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";

        final DataSource viewStoreDataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);
        when(viewStoreDataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(DELETE_STREAM_ERROR_RETRY_SQL)).thenReturn(preparedStatement);

        streamErrorRetryRepository.remove(streamId, source, component);

        final InOrder inOrder = inOrder(preparedStatement, connection);

        inOrder.verify(preparedStatement).setObject(1, streamId);
        inOrder.verify(preparedStatement).setString(2, source);
        inOrder.verify(preparedStatement).setString(3, component);
        inOrder.verify(preparedStatement).executeUpdate();
        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldThrowStreamErrorPersistenceExceptionIfRemovingStreamErrorRetryFails() throws Exception {

        final SQLException sqlException = new SQLException("Oh no");

        final UUID streamId = fromString("d3604229-07b1-4362-9f77-e447053e1b3b");
        final String source = "some-source";
        final String component = "some-component";

        final DataSource viewStoreDataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);
        when(viewStoreDataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(DELETE_STREAM_ERROR_RETRY_SQL)).thenReturn(preparedStatement);
        doThrow(sqlException).when(preparedStatement).executeUpdate();

        final StreamErrorPersistenceException streamErrorPersistenceException = assertThrows(
                StreamErrorPersistenceException.class,
                () -> streamErrorRetryRepository.remove(streamId, source, component));

        assertThat(streamErrorPersistenceException.getCause(), is(sqlException));
        assertThat(streamErrorPersistenceException.getMessage(), is("Failed to delete stream_error_retry. streamId: 'd3604229-07b1-4362-9f77-e447053e1b3b', source: 'some-source', component: 'some-component'"));

        final InOrder inOrder = inOrder(preparedStatement, connection);

        inOrder.verify(preparedStatement).setObject(1, streamId);
        inOrder.verify(preparedStatement).setString(2, source);
        inOrder.verify(preparedStatement).setString(3, component);
        inOrder.verify(preparedStatement).executeUpdate();
        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }
}