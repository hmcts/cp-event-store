package uk.gov.justice.services.event.buffer.core.repository.streamerror;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ActiveStreamErrorsRepositoryTest {

    @Mock
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    private static final String SELECT_ACTIVE_ERRORS_SQL = """
            SELECT
                hash,
                exception_classname,
                cause_classname,
                java_classname,
                java_method,
                java_line_number,
            (
                SELECT COUNT(DISTINCT stream_error.stream_id)
                FROM stream_error
                WHERE stream_error.hash = stream_error_hash.hash
            ) AS affected_stream_count,
            (
                SELECT SUM(stream_status.latest_known_position - stream_status.stream_error_position)
                FROM stream_status, stream_error
                WHERE stream_error.hash = stream_error_hash.hash and stream_error.id = stream_status.stream_error_id
            ) AS affected_event_count
            FROM stream_error_hash;
            """;

    @InjectMocks
    private ActiveStreamErrorsRepository activeStreamErrorsRepository;

    @Test
    public void shouldGetTheListOfActiveErrors() throws Exception {

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(SELECT_ACTIVE_ERRORS_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, true, false);

        when(resultSet.getString("hash")).thenReturn("hash_1", "hash_2");
        when(resultSet.getString("exception_classname")).thenReturn("exception_1", "exception_2");
        when(resultSet.getString("cause_classname")).thenReturn("cause_1", "cause_2");
        when(resultSet.getString("java_classname")).thenReturn("classname_1", "classname_2");
        when(resultSet.getString("java_method")).thenReturn("method_1", "method_2");
        when(resultSet.getInt("java_line_number")).thenReturn(11, 22);
        when(resultSet.getInt("affected_stream_count")).thenReturn(111, 222);
        when(resultSet.getInt("affected_event_count")).thenReturn(1111, 2222);

        final List<ActiveStreamError> activeStreamErrors = activeStreamErrorsRepository.getActiveStreamErrors();

        assertThat(activeStreamErrors.size(), is(2));

        assertThat(activeStreamErrors.get(0).hash(), is("hash_1"));
        assertThat(activeStreamErrors.get(0).exceptionClassname(), is("exception_1"));
        assertThat(activeStreamErrors.get(0).causeClassname(), is(of("cause_1")));
        assertThat(activeStreamErrors.get(0).javaClassname(), is("classname_1"));
        assertThat(activeStreamErrors.get(0).javaMethod(), is("method_1"));
        assertThat(activeStreamErrors.get(0).javaLineNumber(), is(11));
        assertThat(activeStreamErrors.get(0).affectedStreamsCount(), is(111));
        assertThat(activeStreamErrors.get(0).affectedEventsCount(), is(1111));

        assertThat(activeStreamErrors.get(1).hash(), is("hash_2"));
        assertThat(activeStreamErrors.get(1).exceptionClassname(), is("exception_2"));
        assertThat(activeStreamErrors.get(1).causeClassname(), is(of("cause_2")));
        assertThat(activeStreamErrors.get(1).javaClassname(), is("classname_2"));
        assertThat(activeStreamErrors.get(1).javaMethod(), is("method_2"));
        assertThat(activeStreamErrors.get(1).javaLineNumber(), is(22));
        assertThat(activeStreamErrors.get(1).affectedStreamsCount(), is(222));
        assertThat(activeStreamErrors.get(1).affectedEventsCount(), is(2222));

        verify(connection).close();
        verify(preparedStatement).close();
        verify(resultSet).close();
    }

    @Test
    public void shouldConvertNullCauseClassNamesToEmpty() throws Exception {

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(SELECT_ACTIVE_ERRORS_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, true, false);

        when(resultSet.getString("hash")).thenReturn("hash_1", "hash_2");
        when(resultSet.getString("exception_classname")).thenReturn("exception_1", "exception_2");
        when(resultSet.getString("cause_classname")).thenReturn(null, null);
        when(resultSet.getString("java_classname")).thenReturn("classname_1", "classname_2");
        when(resultSet.getString("java_method")).thenReturn("method_1", "method_2");
        when(resultSet.getInt("java_line_number")).thenReturn(11, 22);
        when(resultSet.getInt("affected_stream_count")).thenReturn(111, 222);
        when(resultSet.getInt("affected_event_count")).thenReturn(1111, 2222);

        final List<ActiveStreamError> activeStreamErrors = activeStreamErrorsRepository.getActiveStreamErrors();

        assertThat(activeStreamErrors.size(), is(2));

        assertThat(activeStreamErrors.get(0).hash(), is("hash_1"));
        assertThat(activeStreamErrors.get(0).exceptionClassname(), is("exception_1"));
        assertThat(activeStreamErrors.get(0).causeClassname(), is(empty()));
        assertThat(activeStreamErrors.get(0).javaClassname(), is("classname_1"));
        assertThat(activeStreamErrors.get(0).javaMethod(), is("method_1"));
        assertThat(activeStreamErrors.get(0).javaLineNumber(), is(11));
        assertThat(activeStreamErrors.get(0).affectedStreamsCount(), is(111));
        assertThat(activeStreamErrors.get(0).affectedEventsCount(), is(1111));

        assertThat(activeStreamErrors.get(1).hash(), is("hash_2"));
        assertThat(activeStreamErrors.get(1).exceptionClassname(), is("exception_2"));
        assertThat(activeStreamErrors.get(1).causeClassname(), is(empty()));
        assertThat(activeStreamErrors.get(1).javaClassname(), is("classname_2"));
        assertThat(activeStreamErrors.get(1).javaMethod(), is("method_2"));
        assertThat(activeStreamErrors.get(1).javaLineNumber(), is(22));
        assertThat(activeStreamErrors.get(1).affectedStreamsCount(), is(222));
        assertThat(activeStreamErrors.get(1).affectedEventsCount(), is(2222));

        verify(connection).close();
        verify(preparedStatement).close();
        verify(resultSet).close();
    }

    @Test
    public void shouldThrowStreamErrorPersistenceExceptionIfGettingListOfActiveErrorsFails() throws Exception {

        final SQLException sqlException = new SQLException("Ooopd");

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(SELECT_ACTIVE_ERRORS_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenThrow(sqlException);

        final StreamErrorPersistenceException streamErrorPersistenceException = assertThrows(
                StreamErrorPersistenceException.class,
                () -> activeStreamErrorsRepository.getActiveStreamErrors());

        assertThat(streamErrorPersistenceException.getCause(), is(sqlException));
        assertThat(streamErrorPersistenceException.getMessage(), is("Failed to get active stream errors from viewstore error tables"));

        verify(connection).close();
        verify(preparedStatement).close();
        verify(resultSet).close();
    }
}