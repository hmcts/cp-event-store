package uk.gov.justice.services.test.utils.persistence;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.jdbc.persistence.DataAccessException;

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
public class SequenceSetterTest {

    @InjectMocks
    private SequenceSetter sequenceSetter;

    @Test
    public void shouldSetSequenceNumberOnSequence() throws Exception {

        final long startNumber = 23L;
        final String sequenceName = "some-database-sequence";
        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement("ALTER SEQUENCE some-database-sequence RESTART WITH 23")).thenReturn(preparedStatement);

        sequenceSetter.setSequenceTo(startNumber, sequenceName, dataSource);

        verify(preparedStatement).executeUpdate();
    }

    @Test
    public void shouldThrowDataAccessExceptionIfSettingSequenceNumberFails() throws Exception {

        final long startNumber = 23L;
        final String sequenceName = "some-database-sequence";
        final SQLException sqlException = new SQLException("Ooops");

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement("ALTER SEQUENCE some-database-sequence RESTART WITH 23")).thenReturn(preparedStatement);
        doThrow(sqlException).when(preparedStatement).executeUpdate();

        final DataAccessException dataAccessException = assertThrows(
                DataAccessException.class,
                () -> sequenceSetter.setSequenceTo(startNumber, sequenceName, dataSource));

       assertThat(dataAccessException.getCause(), is(sqlException));
       assertThat(dataAccessException.getMessage(), is("Failed to set 'some-database-sequence' sequence to 23"));
    }

    @Test
    public void shouldGetCurrentSequenceValue() throws Exception {

        final long sequenceValue = 23L;
        final String sequenceName = "some-database-sequence";

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement("SELECT LAST_VALUE FROM some-database-sequence")).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getLong(1)).thenReturn(sequenceValue);

        assertThat(sequenceSetter.getCurrentSequenceValue(sequenceName, dataSource), is(sequenceValue));
    }

    @Test
    public void shouldThrowDataAccessExceptionIfGettingCurrentSequenceValueFails() throws Exception {

        final String sequenceName = "some-database-sequence";
        final SQLException sqlException = new SQLException("Ooops");

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement("SELECT LAST_VALUE FROM some-database-sequence")).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getLong(1)).thenThrow(sqlException);

        final DataAccessException dataAccessException = assertThrows(
                DataAccessException.class,
                () -> sequenceSetter.getCurrentSequenceValue(sequenceName, dataSource));

        assertThat(dataAccessException.getCause(), is(sqlException));
        assertThat(dataAccessException.getMessage(), is("Failed to get current value from 'some-database-sequence' sequence"));
    }

    @Test
    public void shouldGetNextSequenceValue() throws Exception {

        final long nextValue = 23L;
        final String sequenceName = "some-database-sequence";

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement("SELECT nextval('some-database-sequence')")).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getLong(1)).thenReturn(nextValue);

        assertThat(sequenceSetter.getNextSequenceValue(sequenceName, dataSource), is(nextValue));
    }

    @Test
    public void shouldThrowDataAccessExceptionIfGettingNextSequenceValueFails() throws Exception {

        final String sequenceName = "some-database-sequence";
        final SQLException sqlException = new SQLException("Ooops");

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement("SELECT nextval('some-database-sequence')")).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getLong(1)).thenThrow(sqlException);

        final DataAccessException dataAccessException = assertThrows(
                DataAccessException.class,
                () -> sequenceSetter.getNextSequenceValue(sequenceName, dataSource));

        assertThat(dataAccessException.getCause(), is(sqlException));
        assertThat(dataAccessException.getMessage(), is("Failed to current value from 'some-database-sequence' sequence"));
    }
}