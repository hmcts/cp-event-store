package uk.gov.justice.services.eventsourcing.publishedevent.jdbc;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.eventsourcing.publishedevent.jdbc.EventNumberSequenceDataAccess.ADVISORY_LOCK_KEY;
import static uk.gov.justice.services.eventsourcing.publishedevent.jdbc.EventNumberSequenceDataAccess.SELECT_NEXT_EVENT_NUMBER_SQL;
import static uk.gov.justice.services.eventsourcing.publishedevent.jdbc.EventNumberSequenceDataAccess.TRANSACTION_LEVEL_ADVISORY_LOCK_SQL;
import static uk.gov.justice.services.eventsourcing.publishedevent.jdbc.EventNumberSequenceDataAccess.UPDATE_NEXT_EVENT_NUMBER_SQL;

import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EventNumberSequenceDataAccessTest {

    @Mock
    private EventStoreDataSourceProvider eventStoreDataSourceProvider;

    @InjectMocks
    private EventNumberSequenceDataAccess eventNumberSequenceDataAccess;

    @Test
    public void shouldLockWithAdvisoryLockAndGetTheNextEventNumber() throws Exception {

        final Long nextValue = 234L;

        final Connection connection = mock(Connection.class);
        final DataSource defaultDataSource = mock(DataSource.class);
        final PreparedStatement lockPreparedStatement = mock(PreparedStatement.class);
        final PreparedStatement queryPreparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(defaultDataSource);
        when(defaultDataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(TRANSACTION_LEVEL_ADVISORY_LOCK_SQL)).thenReturn(lockPreparedStatement);
        when(connection.prepareStatement(SELECT_NEXT_EVENT_NUMBER_SQL)).thenReturn(queryPreparedStatement);
        when(queryPreparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getLong("next_value")).thenReturn(nextValue);

        assertThat(eventNumberSequenceDataAccess.lockAndGetNextAvailableEventNumber(), is(nextValue));

        final InOrder inOrder = inOrder(lockPreparedStatement, queryPreparedStatement, resultSet, connection);
        inOrder.verify(lockPreparedStatement).setLong(1, ADVISORY_LOCK_KEY);
        inOrder.verify(lockPreparedStatement).execute();
        inOrder.verify(resultSet).close();
        inOrder.verify(queryPreparedStatement).close();
        inOrder.verify(lockPreparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldThrowEventNumberSequenceExceptionWhenGettingTheNextEventNumber() throws Exception {

        final SQLException sqlException = new SQLException("Oops");

        final Connection connection = mock(Connection.class);
        final DataSource defaultDataSource = mock(DataSource.class);
        final PreparedStatement lockPreparedStatement = mock(PreparedStatement.class);
        final PreparedStatement queryPreparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(defaultDataSource);
        when(defaultDataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(TRANSACTION_LEVEL_ADVISORY_LOCK_SQL)).thenReturn(lockPreparedStatement);
        when(connection.prepareStatement(SELECT_NEXT_EVENT_NUMBER_SQL)).thenReturn(queryPreparedStatement);
        when(queryPreparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getLong("next_value")).thenThrow(sqlException);

        final EventNumberSequenceException eventNumberSequenceException = assertThrows(
                EventNumberSequenceException.class,
                () -> eventNumberSequenceDataAccess.lockAndGetNextAvailableEventNumber());

        assertThat(eventNumberSequenceException.getMessage(), is("Failed to get next event number sequence"));
        assertThat(eventNumberSequenceException.getCause(), is(sqlException));

        final InOrder inOrder = inOrder(lockPreparedStatement, queryPreparedStatement, resultSet, connection);
        inOrder.verify(lockPreparedStatement).setLong(1, ADVISORY_LOCK_KEY);
        inOrder.verify(lockPreparedStatement).execute();
        inOrder.verify(resultSet).close();
        inOrder.verify(queryPreparedStatement).close();
        inOrder.verify(lockPreparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldUpdateNextEventNumberSequence() throws Exception {

        final Long nextEventNumber = 987324L;

        final Connection connection = mock(Connection.class);
        final DataSource defaultDataSource = mock(DataSource.class);
        final PreparedStatement lockPreparedStatement = mock(PreparedStatement.class);
        final PreparedStatement queryPreparedStatement = mock(PreparedStatement.class);

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(defaultDataSource);
        when(defaultDataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(TRANSACTION_LEVEL_ADVISORY_LOCK_SQL)).thenReturn(lockPreparedStatement);
        when(connection.prepareStatement(UPDATE_NEXT_EVENT_NUMBER_SQL)).thenReturn(queryPreparedStatement);

        eventNumberSequenceDataAccess.updateNextAvailableEventNumberTo(nextEventNumber);

        final InOrder inOrder = inOrder(lockPreparedStatement, queryPreparedStatement, connection);
        inOrder.verify(lockPreparedStatement).setLong(1, ADVISORY_LOCK_KEY);
        inOrder.verify(lockPreparedStatement).execute();
        inOrder.verify(queryPreparedStatement).setLong(1, nextEventNumber);
        inOrder.verify(queryPreparedStatement).execute();
        inOrder.verify(queryPreparedStatement).close();
        inOrder.verify(lockPreparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldThrowEventNumberSequenceExceptionIfUpdatingNextEventNumberSequenceFails() throws Exception {

        final Long nextEventNumber = 987324L;
        final SQLException sqlException = new SQLException("Ooops");

        final Connection connection = mock(Connection.class);
        final DataSource defaultDataSource = mock(DataSource.class);
        final PreparedStatement lockPreparedStatement = mock(PreparedStatement.class);
        final PreparedStatement queryPreparedStatement = mock(PreparedStatement.class, "fred");

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(defaultDataSource);
        when(defaultDataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(TRANSACTION_LEVEL_ADVISORY_LOCK_SQL)).thenReturn(lockPreparedStatement);
        when(connection.prepareStatement(UPDATE_NEXT_EVENT_NUMBER_SQL)).thenReturn(queryPreparedStatement);
        when(queryPreparedStatement.execute()).thenThrow(sqlException);

        final EventNumberSequenceException eventNumberSequenceException = assertThrows(
                EventNumberSequenceException.class,
                () -> eventNumberSequenceDataAccess.updateNextAvailableEventNumberTo(nextEventNumber));

        assertThat(eventNumberSequenceException.getMessage(), is("Failed to update next event number sequence to 987324"));
        assertThat(eventNumberSequenceException.getCause(), is(sqlException));

        final InOrder inOrder = inOrder(lockPreparedStatement, queryPreparedStatement, connection);
        inOrder.verify(lockPreparedStatement).setLong(1, ADVISORY_LOCK_KEY);
        inOrder.verify(lockPreparedStatement).execute();
        inOrder.verify(queryPreparedStatement).setLong(1, nextEventNumber);
        inOrder.verify(queryPreparedStatement).execute();
        inOrder.verify(queryPreparedStatement).close();
        inOrder.verify(lockPreparedStatement).close();
        inOrder.verify(connection).close();
    }
}