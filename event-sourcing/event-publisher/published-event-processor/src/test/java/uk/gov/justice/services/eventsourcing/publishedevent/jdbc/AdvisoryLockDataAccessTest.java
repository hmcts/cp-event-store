package uk.gov.justice.services.eventsourcing.publishedevent.jdbc;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.eventsourcing.publishedevent.jdbc.AdvisoryLockDataAccess.BLOCKING_TRANSACTION_LEVEL_ADVISORY_LOCK_SQL;
import static uk.gov.justice.services.eventsourcing.publishedevent.jdbc.AdvisoryLockDataAccess.NON_BLOCKING_TRANSACTION_LEVEL_ADVISORY_LOCK_SQL;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class AdvisoryLockDataAccessTest {

    @InjectMocks
    private AdvisoryLockDataAccess advisoryLockDataAccess;

    @Test
    public void shouldObtainBlockingAdvisoryLock() throws Exception {

        final Long advisoryLockId = 1L;

        final Connection connection = mock(Connection.class);
        final PreparedStatement lockPreparedStatement = mock(PreparedStatement.class);

        when(connection.prepareStatement(BLOCKING_TRANSACTION_LEVEL_ADVISORY_LOCK_SQL)).thenReturn(lockPreparedStatement);

        advisoryLockDataAccess.obtainBlockingTransactionLevelAdvisoryLock(connection, advisoryLockId);

        final InOrder inOrder = inOrder(lockPreparedStatement);
        inOrder.verify(lockPreparedStatement).setLong(1, advisoryLockId);
        inOrder.verify(lockPreparedStatement).execute();
        inOrder.verify(lockPreparedStatement).close();
    }

    @Test
    public void shouldThrowAdvisoryLockExceptionIfObtainingBlockingAdvisoryLockFails() throws Exception {

        final Long advisoryLockId = 1L;
        final SQLException sqlException = new SQLException("Oops");

        final Connection connection = mock(Connection.class);
        final PreparedStatement lockPreparedStatement = mock(PreparedStatement.class);

        when(connection.prepareStatement(BLOCKING_TRANSACTION_LEVEL_ADVISORY_LOCK_SQL)).thenReturn(lockPreparedStatement);
        when(lockPreparedStatement.execute()).thenThrow(sqlException);

        final AdvisoryLockException advisoryLockException = assertThrows(
                AdvisoryLockException.class,
                () -> advisoryLockDataAccess.obtainBlockingTransactionLevelAdvisoryLock(connection, advisoryLockId));

        assertThat(advisoryLockException.getCause(), is(sqlException));
        assertThat(advisoryLockException.getMessage(), is("Failed to obtain blocking advisory lock on postgres with lock key '1'"));

        final InOrder inOrder = inOrder(lockPreparedStatement);
        inOrder.verify(lockPreparedStatement).setLong(1, advisoryLockId);
        inOrder.verify(lockPreparedStatement).execute();
        inOrder.verify(lockPreparedStatement).close();
    }

    @Test
    public void shouldObtainNonBlockingAdvisoryLock() throws Exception {

        final Long advisoryLockId = 1L;
        final boolean lockObtained = false;

        final Connection connection = mock(Connection.class);
        final PreparedStatement lockPreparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(connection.prepareStatement(NON_BLOCKING_TRANSACTION_LEVEL_ADVISORY_LOCK_SQL)).thenReturn(lockPreparedStatement);
        when(lockPreparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean(1)).thenReturn(lockObtained);

        assertThat(advisoryLockDataAccess.tryNonBlockingTransactionLevelAdvisoryLock(connection, advisoryLockId), is(lockObtained));

        final InOrder inOrder = inOrder(lockPreparedStatement, resultSet);
        inOrder.verify(lockPreparedStatement).setLong(1, advisoryLockId);
        inOrder.verify(lockPreparedStatement).executeQuery();
        inOrder.verify(resultSet).next();
        inOrder.verify(resultSet).getBoolean(1);
        inOrder.verify(resultSet).close();
        inOrder.verify(lockPreparedStatement).close();
    }

    @Test
    public void shouldThrowAdvisoryLockExceptionIfObtainingNonBlockingLockReturnsNoResults() throws Exception {

        final Long advisoryLockId = 1L;

        final Connection connection = mock(Connection.class);
        final PreparedStatement lockPreparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(connection.prepareStatement(NON_BLOCKING_TRANSACTION_LEVEL_ADVISORY_LOCK_SQL)).thenReturn(lockPreparedStatement);
        when(lockPreparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        final AdvisoryLockException advisoryLockException = assertThrows(
                AdvisoryLockException.class,
                () -> advisoryLockDataAccess.tryNonBlockingTransactionLevelAdvisoryLock(connection, advisoryLockId));

        assertThat(advisoryLockException.getMessage(), is("Unable to obtain non-blocking advisory lock for id '1'. No result returned when obtaining lock"));

        final InOrder inOrder = inOrder(lockPreparedStatement);
        inOrder.verify(lockPreparedStatement).close();
    }

    @Test
    public void shouldThrowAdvisoryLockExceptionIfObtainingNonBlockingLockFails() throws Exception {

        final Long advisoryLockId = 1L;
        final SQLException sqlException = new SQLException("Ooops");

        final Connection connection = mock(Connection.class);
        final PreparedStatement lockPreparedStatement = mock(PreparedStatement.class);

        when(connection.prepareStatement(NON_BLOCKING_TRANSACTION_LEVEL_ADVISORY_LOCK_SQL)).thenReturn(lockPreparedStatement);
        when(lockPreparedStatement.executeQuery()).thenThrow(sqlException);

        final AdvisoryLockException advisoryLockException = assertThrows(
                AdvisoryLockException.class,
                () -> advisoryLockDataAccess.tryNonBlockingTransactionLevelAdvisoryLock(connection, advisoryLockId));

        assertThat(advisoryLockException.getMessage(), is("Failed to obtain non-blocking advisory lock on database with lock key '1'"));

        final InOrder inOrder = inOrder(lockPreparedStatement);
        inOrder.verify(lockPreparedStatement).close();
    }
}