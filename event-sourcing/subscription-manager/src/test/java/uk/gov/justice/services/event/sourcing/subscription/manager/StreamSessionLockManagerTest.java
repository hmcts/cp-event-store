package uk.gov.justice.services.event.sourcing.subscription.manager;

import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class StreamSessionLockManagerTest {

    @Mock
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    @Mock
    private Logger logger;

    @InjectMocks
    private StreamSessionLockManager streamSessionLockManager;

    @Test
    public void shouldOpenConnectionFromViewStoreDataSource() throws Exception {
        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);

        final Connection result = streamSessionLockManager.openLockConnection();

        assertThat(result, is(connection));
        verify(connection).setAutoCommit(true);
    }

    @Test
    public void shouldThrowStreamSessionLockExceptionWhenOpenConnectionFails() throws Exception {
        final DataSource dataSource = mock(DataSource.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenThrow(new SQLException("Connection failed"));

        assertThrows(StreamSessionLockException.class, () -> streamSessionLockManager.openLockConnection());
    }

    @Test
    public void shouldAcquireAdvisoryLockForStream() throws Exception {
        final UUID streamId = randomUUID();
        final long lockKey = streamId.getLeastSignificantBits();
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(connection.prepareStatement(StreamSessionLockManager.TRY_SESSION_ADVISORY_LOCK_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean(1)).thenReturn(true);

        final boolean locked = streamSessionLockManager.tryLockStream(connection, streamId);

        assertThat(locked, is(true));
        verify(preparedStatement).setLong(1, lockKey);
    }

    @Test
    public void shouldReturnFalseWhenAdvisoryLockAlreadyHeld() throws Exception {
        final UUID streamId = randomUUID();
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(connection.prepareStatement(StreamSessionLockManager.TRY_SESSION_ADVISORY_LOCK_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean(1)).thenReturn(false);

        final boolean locked = streamSessionLockManager.tryLockStream(connection, streamId);

        assertThat(locked, is(false));
    }

    @Test
    public void shouldThrowExceptionWhenNoResultReturnedFromAdvisoryLock() throws Exception {
        final UUID streamId = randomUUID();
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(connection.prepareStatement(StreamSessionLockManager.TRY_SESSION_ADVISORY_LOCK_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        assertThrows(StreamSessionLockException.class, () -> streamSessionLockManager.tryLockStream(connection, streamId));
    }

    @Test
    public void shouldThrowExceptionWhenAdvisoryLockSqlFails() throws Exception {
        final UUID streamId = randomUUID();
        final Connection connection = mock(Connection.class);

        when(connection.prepareStatement(anyString())).thenThrow(new SQLException("SQL error"));

        assertThrows(StreamSessionLockException.class, () -> streamSessionLockManager.tryLockStream(connection, streamId));
    }

    @Test
    public void shouldUnlockAdvisoryLockForStream() throws Exception {
        final UUID streamId = randomUUID();
        final long lockKey = streamId.getLeastSignificantBits();
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(connection.prepareStatement(StreamSessionLockManager.SESSION_ADVISORY_UNLOCK_SQL)).thenReturn(preparedStatement);

        streamSessionLockManager.unlockStream(connection, streamId);

        verify(preparedStatement).setLong(1, lockKey);
        verify(preparedStatement).execute();
    }

    @Test
    public void shouldLogWarningWhenUnlockFails() throws Exception {
        final UUID streamId = randomUUID();
        final Connection connection = mock(Connection.class);

        when(connection.prepareStatement(anyString())).thenThrow(new SQLException("Unlock failed"));

        streamSessionLockManager.unlockStream(connection, streamId);

        verify(logger).warn("Failed to release advisory lock for stream '{}': {}", streamId, "Unlock failed");
    }

    @Test
    public void shouldCloseConnectionQuietly() throws Exception {
        final Connection connection = mock(Connection.class);

        streamSessionLockManager.closeQuietly(connection);

        verify(connection).close();
    }

    @Test
    public void shouldHandleNullConnectionInCloseQuietly() {
        streamSessionLockManager.closeQuietly(null);
    }

    @Test
    public void shouldLogWarningWhenCloseConnectionFails() throws Exception {
        final Connection connection = mock(Connection.class);
        doThrow(new SQLException("Close failed")).when(connection).close();

        streamSessionLockManager.closeQuietly(connection);

        verify(logger).warn("Failed to close advisory lock connection: {}", "Close failed");
    }
}
