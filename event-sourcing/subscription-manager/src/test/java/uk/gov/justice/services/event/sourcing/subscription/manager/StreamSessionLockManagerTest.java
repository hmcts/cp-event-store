package uk.gov.justice.services.event.sourcing.subscription.manager;

import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;

import java.util.Objects;
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

    private static final String SOURCE = "some-source";
    private static final String COMPONENT = "some-component";

    @Mock
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    @Mock
    private Logger logger;

    @InjectMocks
    private StreamSessionLockManager streamSessionLockManager;

    @Test
    public void shouldAcquireLockAndReturnAcquiredStreamLock() throws Exception {
        final UUID streamId = randomUUID();
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        mockConnectionFromDataSource(connection);
        when(connection.prepareStatement(StreamSessionLockManager.TRY_SESSION_ADVISORY_LOCK_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean(1)).thenReturn(true);

        final StreamSessionLockManager.StreamSessionLock lock = streamSessionLockManager.lockStream(streamId, SOURCE, COMPONENT);

        assertThat(lock.isAcquired(), is(true));
        verify(preparedStatement).setInt(1, Objects.hash(SOURCE, COMPONENT));
        verify(preparedStatement).setInt(2, (int) streamId.getLeastSignificantBits());
    }

    @Test
    public void shouldReturnNotAcquiredStreamLockWhenAdvisoryLockAlreadyHeld() throws Exception {
        final UUID streamId = randomUUID();
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        mockConnectionFromDataSource(connection);
        when(connection.prepareStatement(StreamSessionLockManager.TRY_SESSION_ADVISORY_LOCK_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean(1)).thenReturn(false);

        final StreamSessionLockManager.StreamSessionLock lock = streamSessionLockManager.lockStream(streamId, SOURCE, COMPONENT);

        assertThat(lock.isAcquired(), is(false));
        verify(logger).warn("Advisory lock contention detected for stream '{}', source '{}', component '{}'", streamId, SOURCE, COMPONENT);
    }

    @Test
    public void shouldThrowExceptionAndCloseConnectionWhenLockSqlFails() throws Exception {
        final UUID streamId = randomUUID();
        final Connection connection = mock(Connection.class);

        mockConnectionFromDataSource(connection);
        when(connection.prepareStatement(anyString())).thenThrow(new SQLException("SQL error"));

        assertThrows(StreamSessionLockException.class, () -> streamSessionLockManager.lockStream(streamId, SOURCE, COMPONENT));

        verify(connection).close();
    }

    @Test
    public void shouldThrowExceptionWhenGetConnectionFails() throws Exception {
        final DataSource dataSource = mock(DataSource.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenThrow(new SQLException("Connection failed"));

        assertThrows(StreamSessionLockException.class, () -> streamSessionLockManager.lockStream(randomUUID(), SOURCE, COMPONENT));
    }

    @Test
    public void shouldUnlockAndCloseConnectionOnCloseWhenLockWasAcquired() throws Exception {
        final UUID streamId = randomUUID();
        final Connection connection = mock(Connection.class);
        final PreparedStatement lockStatement = mock(PreparedStatement.class);
        final ResultSet lockResultSet = mock(ResultSet.class);
        final PreparedStatement unlockStatement = mock(PreparedStatement.class);
        final ResultSet unlockResultSet = mock(ResultSet.class);

        mockConnectionFromDataSource(connection);
        when(connection.prepareStatement(StreamSessionLockManager.TRY_SESSION_ADVISORY_LOCK_SQL)).thenReturn(lockStatement);
        when(lockStatement.executeQuery()).thenReturn(lockResultSet);
        when(lockResultSet.next()).thenReturn(true);
        when(lockResultSet.getBoolean(1)).thenReturn(true);

        when(connection.prepareStatement(StreamSessionLockManager.SESSION_ADVISORY_UNLOCK_SQL)).thenReturn(unlockStatement);
        when(unlockStatement.executeQuery()).thenReturn(unlockResultSet);
        when(unlockResultSet.next()).thenReturn(true);
        when(unlockResultSet.getBoolean(1)).thenReturn(true);

        final StreamSessionLockManager.StreamSessionLock lock = streamSessionLockManager.lockStream(streamId, SOURCE, COMPONENT);
        lock.close();

        verify(unlockStatement).setInt(1, Objects.hash(SOURCE, COMPONENT));
        verify(unlockStatement).setInt(2, (int) streamId.getLeastSignificantBits());
        verify(connection).close();
    }

    @Test
    public void shouldOnlyCloseConnectionOnCloseWhenLockWasNotAcquired() throws Exception {
        final UUID streamId = randomUUID();
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        mockConnectionFromDataSource(connection);
        when(connection.prepareStatement(StreamSessionLockManager.TRY_SESSION_ADVISORY_LOCK_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean(1)).thenReturn(false);

        final StreamSessionLockManager.StreamSessionLock lock = streamSessionLockManager.lockStream(streamId, SOURCE, COMPONENT);
        lock.close();

        verify(connection, never()).prepareStatement(StreamSessionLockManager.SESSION_ADVISORY_UNLOCK_SQL);
        verify(connection).close();
    }

    @Test
    public void shouldLogWarningWhenUnlockReturnsFalse() throws Exception {
        final UUID streamId = randomUUID();
        final Connection connection = mock(Connection.class);
        final PreparedStatement lockStatement = mock(PreparedStatement.class);
        final ResultSet lockResultSet = mock(ResultSet.class);
        final PreparedStatement unlockStatement = mock(PreparedStatement.class);
        final ResultSet unlockResultSet = mock(ResultSet.class);

        mockConnectionFromDataSource(connection);
        when(connection.prepareStatement(StreamSessionLockManager.TRY_SESSION_ADVISORY_LOCK_SQL)).thenReturn(lockStatement);
        when(lockStatement.executeQuery()).thenReturn(lockResultSet);
        when(lockResultSet.next()).thenReturn(true);
        when(lockResultSet.getBoolean(1)).thenReturn(true);

        when(connection.prepareStatement(StreamSessionLockManager.SESSION_ADVISORY_UNLOCK_SQL)).thenReturn(unlockStatement);
        when(unlockStatement.executeQuery()).thenReturn(unlockResultSet);
        when(unlockResultSet.next()).thenReturn(true);
        when(unlockResultSet.getBoolean(1)).thenReturn(false);

        final StreamSessionLockManager.StreamSessionLock lock = streamSessionLockManager.lockStream(streamId, SOURCE, COMPONENT);
        lock.close();

        verify(logger).warn("Advisory unlock returned false for stream '{}' — lock was not held", streamId);
        verify(connection).close();
    }

    @Test
    public void shouldLogWarningWhenUnlockReturnsNoResult() throws Exception {
        final UUID streamId = randomUUID();
        final Connection connection = mock(Connection.class);
        final PreparedStatement lockStatement = mock(PreparedStatement.class);
        final ResultSet lockResultSet = mock(ResultSet.class);
        final PreparedStatement unlockStatement = mock(PreparedStatement.class);
        final ResultSet unlockResultSet = mock(ResultSet.class);

        mockConnectionFromDataSource(connection);
        when(connection.prepareStatement(StreamSessionLockManager.TRY_SESSION_ADVISORY_LOCK_SQL)).thenReturn(lockStatement);
        when(lockStatement.executeQuery()).thenReturn(lockResultSet);
        when(lockResultSet.next()).thenReturn(true);
        when(lockResultSet.getBoolean(1)).thenReturn(true);

        when(connection.prepareStatement(StreamSessionLockManager.SESSION_ADVISORY_UNLOCK_SQL)).thenReturn(unlockStatement);
        when(unlockStatement.executeQuery()).thenReturn(unlockResultSet);
        when(unlockResultSet.next()).thenReturn(false);

        final StreamSessionLockManager.StreamSessionLock lock = streamSessionLockManager.lockStream(streamId, SOURCE, COMPONENT);
        lock.close();

        verify(logger).warn("No result returned when releasing advisory lock for stream '{}'", streamId);
        verify(connection).close();
    }

    @Test
    public void shouldCloseConnectionEvenWhenUnlockFails() throws Exception {
        final UUID streamId = randomUUID();
        final Connection connection = mock(Connection.class);
        final PreparedStatement lockStatement = mock(PreparedStatement.class);
        final ResultSet lockResultSet = mock(ResultSet.class);

        mockConnectionFromDataSource(connection);
        when(connection.prepareStatement(StreamSessionLockManager.TRY_SESSION_ADVISORY_LOCK_SQL)).thenReturn(lockStatement);
        when(lockStatement.executeQuery()).thenReturn(lockResultSet);
        when(lockResultSet.next()).thenReturn(true);
        when(lockResultSet.getBoolean(1)).thenReturn(true);

        when(connection.prepareStatement(StreamSessionLockManager.SESSION_ADVISORY_UNLOCK_SQL)).thenThrow(new SQLException("Unlock failed"));

        final StreamSessionLockManager.StreamSessionLock lock = streamSessionLockManager.lockStream(streamId, SOURCE, COMPONENT);
        lock.close();

        verify(logger).warn("Failed to release advisory lock for stream '{}': {}", streamId, "Unlock failed");
        verify(connection).close();
    }

    private void mockConnectionFromDataSource(final Connection connection) throws SQLException {
        final DataSource dataSource = mock(DataSource.class);
        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
    }
}