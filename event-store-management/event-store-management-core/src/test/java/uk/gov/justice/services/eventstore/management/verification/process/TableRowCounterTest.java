package uk.gov.justice.services.eventstore.management.verification.process;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;


@ExtendWith(MockitoExtension.class)
public class TableRowCounterTest {

    @InjectMocks
    private TableRowCounter tableRowCounter;

    @Test
    public void shouldCountTheRowsInTheSpecifiedTable() throws Exception {

        final String tableName = "event_log";
        final int rowCount = 2347;

        final DataSource eventStoreDataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(eventStoreDataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement("SELECT count(*) FROM event_log")).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getInt(1)).thenReturn(rowCount);

        assertThat(tableRowCounter.countRowsIn(tableName, eventStoreDataSource), is(rowCount));
    }

    @Test
    public void shouldFailIfCountingTheRowsInTheSpecifiedTableFails() throws Exception {

        final SQLException sqlException = new SQLException("Ooops");

        final String tableName = "event_log";

        final DataSource eventStoreDataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(eventStoreDataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement("SELECT count(*) FROM event_log")).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getInt(1)).thenThrow(sqlException);

        try {
            tableRowCounter.countRowsIn(tableName, eventStoreDataSource);
            fail();
        } catch (final CatchupVerificationException expected) {
            assertThat(expected.getCause(), is(sqlException));
            assertThat(expected.getMessage(), is("Failed to count rows in event_log table"));
        }
    }

    @Test
    public void shouldFailIfNoResultsReturned() throws Exception {

        final String tableName = "event_log";

        final DataSource eventStoreDataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(eventStoreDataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement("SELECT count(*) FROM event_log")).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        try {
            tableRowCounter.countRowsIn(tableName, eventStoreDataSource);
            fail();
        } catch (final CatchupVerificationException expected) {
            assertThat(expected.getMessage(), is("Counting rows in event_log table did not return any results"));
        }
    }
}
