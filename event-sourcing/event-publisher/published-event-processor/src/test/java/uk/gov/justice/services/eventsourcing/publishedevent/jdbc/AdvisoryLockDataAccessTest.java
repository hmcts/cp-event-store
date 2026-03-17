package uk.gov.justice.services.eventsourcing.publishedevent.jdbc;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.eventsourcing.publishedevent.jdbc.AdvisoryLockDataAccess.NON_BLOCKING_TRANSACTION_LEVEL_ADVISORY_LOCK_SQL;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

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
    public void shouldObtainNonBlockingAdvisoryLockUsingProvidedConnection() throws Exception {

        final Long advisoryLockId = 42L;

        final Connection connection = mock(Connection.class);
        final PreparedStatement lockPreparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(connection.prepareStatement(NON_BLOCKING_TRANSACTION_LEVEL_ADVISORY_LOCK_SQL)).thenReturn(lockPreparedStatement);
        when(lockPreparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean(1)).thenReturn(true);

        assertThat(advisoryLockDataAccess.tryNonBlockingTransactionLevelAdvisoryLock(connection, advisoryLockId), is(true));

        final InOrder inOrder = inOrder(lockPreparedStatement, resultSet);
        inOrder.verify(lockPreparedStatement).setLong(1, advisoryLockId);
        inOrder.verify(lockPreparedStatement).executeQuery();
        inOrder.verify(resultSet).next();
        inOrder.verify(resultSet).getBoolean(1);
    }

    @Test
    public void shouldReturnFalseWhenLockNotAvailable() throws Exception {

        final Long advisoryLockId = 42L;

        final Connection connection = mock(Connection.class);
        final PreparedStatement lockPreparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(connection.prepareStatement(NON_BLOCKING_TRANSACTION_LEVEL_ADVISORY_LOCK_SQL)).thenReturn(lockPreparedStatement);
        when(lockPreparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean(1)).thenReturn(false);

        assertThat(advisoryLockDataAccess.tryNonBlockingTransactionLevelAdvisoryLock(connection, advisoryLockId), is(false));
    }
}