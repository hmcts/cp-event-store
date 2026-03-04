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
import org.postgresql.PGConnection;
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
    public void shouldGetJtaConnectionFromViewStoreDataSource() throws Exception {
        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);

        final Connection result = streamSessionLockManager.getJtaConnection();

        assertThat(result, is(connection));
    }

    @Test
    public void shouldThrowStreamSessionLockExceptionWhenGetJtaConnectionFails() throws Exception {
        final DataSource dataSource = mock(DataSource.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenThrow(new SQLException("Connection failed"));

        assertThrows(StreamSessionLockException.class, () -> streamSessionLockManager.getJtaConnection());
    }

    @Test
    public void shouldUnwrapPhysicalConnectionFromWrappedConnection() throws Exception {
        final Connection wrappedConnection = mock(Connection.class);
        final PhysicalConnectionStub physicalConnection = mock(PhysicalConnectionStub.class);

        when(wrappedConnection.unwrap(PGConnection.class)).thenReturn(physicalConnection);

        final Connection result = streamSessionLockManager.unwrapPhysicalConnection(wrappedConnection);

        assertThat(result, is(physicalConnection));
    }

    @Test
    public void shouldThrowStreamSessionLockExceptionWhenUnwrapFails() throws Exception {
        final Connection wrappedConnection = mock(Connection.class);

        when(wrappedConnection.unwrap(PGConnection.class)).thenThrow(new SQLException("Unwrap failed"));

        assertThrows(StreamSessionLockException.class, () -> streamSessionLockManager.unwrapPhysicalConnection(wrappedConnection));
    }

    @Test
    public void shouldAcquireAdvisoryLockForStream() throws Exception {
        final UUID streamId = randomUUID();
        final long lockKey = streamId.getLeastSignificantBits();
        final Connection physicalConnection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(physicalConnection.prepareStatement(StreamSessionLockManager.TRY_SESSION_ADVISORY_LOCK_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean(1)).thenReturn(true);

        final boolean locked = streamSessionLockManager.tryLockStream(physicalConnection, streamId);

        assertThat(locked, is(true));
        verify(preparedStatement).setLong(1, lockKey);
    }

    @Test
    public void shouldReturnFalseWhenAdvisoryLockAlreadyHeld() throws Exception {
        final UUID streamId = randomUUID();
        final Connection physicalConnection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(physicalConnection.prepareStatement(StreamSessionLockManager.TRY_SESSION_ADVISORY_LOCK_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean(1)).thenReturn(false);

        final boolean locked = streamSessionLockManager.tryLockStream(physicalConnection, streamId);

        assertThat(locked, is(false));
    }

    @Test
    public void shouldThrowExceptionWhenNoResultReturnedFromAdvisoryLock() throws Exception {
        final UUID streamId = randomUUID();
        final Connection physicalConnection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(physicalConnection.prepareStatement(StreamSessionLockManager.TRY_SESSION_ADVISORY_LOCK_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        assertThrows(StreamSessionLockException.class, () -> streamSessionLockManager.tryLockStream(physicalConnection, streamId));
    }

    @Test
    public void shouldThrowExceptionWhenAdvisoryLockSqlFails() throws Exception {
        final UUID streamId = randomUUID();
        final Connection physicalConnection = mock(Connection.class);

        when(physicalConnection.prepareStatement(anyString())).thenThrow(new SQLException("SQL error"));

        assertThrows(StreamSessionLockException.class, () -> streamSessionLockManager.tryLockStream(physicalConnection, streamId));
    }

    @Test
    public void shouldUnlockAdvisoryLockForStream() throws Exception {
        final UUID streamId = randomUUID();
        final long lockKey = streamId.getLeastSignificantBits();
        final Connection physicalConnection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(physicalConnection.prepareStatement(StreamSessionLockManager.SESSION_ADVISORY_UNLOCK_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean(1)).thenReturn(true);

        streamSessionLockManager.unlockStream(physicalConnection, streamId);

        verify(preparedStatement).setLong(1, lockKey);
        verify(preparedStatement).executeQuery();
    }

    @Test
    public void shouldLogWarningWhenUnlockReturnsFalse() throws Exception {
        final UUID streamId = randomUUID();
        final Connection physicalConnection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(physicalConnection.prepareStatement(StreamSessionLockManager.SESSION_ADVISORY_UNLOCK_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean(1)).thenReturn(false);

        streamSessionLockManager.unlockStream(physicalConnection, streamId);

        verify(logger).warn("Advisory lock for stream '{}' was not held when unlock was attempted", streamId);
    }

    @Test
    public void shouldLogWarningWhenUnlockFails() throws Exception {
        final UUID streamId = randomUUID();
        final Connection physicalConnection = mock(Connection.class);

        when(physicalConnection.prepareStatement(anyString())).thenThrow(new SQLException("Unlock failed"));

        streamSessionLockManager.unlockStream(physicalConnection, streamId);

        verify(logger).warn("Failed to release advisory lock for stream '{}': {}", streamId, "Unlock failed");
    }

    @Test
    public void shouldCloseConnectionHandle() throws Exception {
        final Connection connection = mock(Connection.class);

        streamSessionLockManager.closeHandle(connection);

        verify(connection).close();
    }

    @Test
    public void shouldHandleNullConnectionInCloseHandle() {
        streamSessionLockManager.closeHandle(null);
    }

    @Test
    public void shouldLogWarningWhenCloseHandleFails() throws Exception {
        final Connection connection = mock(Connection.class);
        doThrow(new SQLException("Close failed")).when(connection).close();

        streamSessionLockManager.closeHandle(connection);

        verify(logger).warn("Failed to close connection handle: {}", "Close failed");
    }

    @Test
    public void shouldBeginDirectTransactionByDisablingAutoCommit() throws Exception {
        final Connection connection = mock(Connection.class);

        streamSessionLockManager.beginDirectTransaction(connection);

        verify(connection).setAutoCommit(false);
    }

    @Test
    public void shouldThrowExceptionWhenBeginDirectTransactionFails() throws Exception {
        final Connection connection = mock(Connection.class);
        doThrow(new SQLException("AutoCommit failed")).when(connection).setAutoCommit(false);

        assertThrows(StreamSessionLockException.class, () -> streamSessionLockManager.beginDirectTransaction(connection));
    }

    @Test
    public void shouldCommitDirectTransaction() throws Exception {
        final Connection connection = mock(Connection.class);

        streamSessionLockManager.commitDirectTransaction(connection);

        verify(connection).commit();
    }

    @Test
    public void shouldThrowExceptionWhenCommitDirectTransactionFails() throws Exception {
        final Connection connection = mock(Connection.class);
        doThrow(new SQLException("Commit failed")).when(connection).commit();

        assertThrows(StreamSessionLockException.class, () -> streamSessionLockManager.commitDirectTransaction(connection));
    }

    @Test
    public void shouldRollbackDirectTransaction() throws Exception {
        final Connection connection = mock(Connection.class);

        streamSessionLockManager.rollbackDirectTransaction(connection);

        verify(connection).rollback();
    }

    @Test
    public void shouldLogWarningWhenRollbackDirectTransactionFails() throws Exception {
        final Connection connection = mock(Connection.class);
        doThrow(new SQLException("Rollback failed")).when(connection).rollback();

        streamSessionLockManager.rollbackDirectTransaction(connection);

        verify(logger).warn("Failed to rollback direct transaction: {}", "Rollback failed");
    }

    @Test
    public void shouldRestoreAutoCommit() throws Exception {
        final Connection connection = mock(Connection.class);

        streamSessionLockManager.restoreAutoCommit(connection);

        verify(connection).setAutoCommit(true);
    }

    @Test
    public void shouldLogWarningWhenRestoreAutoCommitFails() throws Exception {
        final Connection connection = mock(Connection.class);
        doThrow(new SQLException("AutoCommit restore failed")).when(connection).setAutoCommit(true);

        streamSessionLockManager.restoreAutoCommit(connection);

        verify(logger).warn("Failed to restore autoCommit on connection: {}", "AutoCommit restore failed");
    }

    /**
     * Test interface that implements both Connection and PGConnection,
     * mirroring the real PgConnection class in the PostgreSQL driver.
     */
    interface PhysicalConnectionStub extends Connection, PGConnection {
    }
}
