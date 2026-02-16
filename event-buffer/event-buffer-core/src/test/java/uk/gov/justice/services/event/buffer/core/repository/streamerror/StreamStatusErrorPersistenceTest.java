package uk.gov.justice.services.event.buffer.core.repository.streamerror;

import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.toSqlTimestamp;

import uk.gov.justice.services.common.util.UtcClock;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StreamStatusErrorPersistenceTest {

    private static final String SELECT_FOR_UPDATE_SQL = """
            SELECT
                position
            FROM
                stream_status
            WHERE stream_id = ?
            AND source = ?
            AND component = ?
            FOR NO KEY UPDATE
            """;

    private static final String UPDATE_STREAM_ERROR_OCCURRED_AT_SQL = """
                UPDATE stream_error
                SET occurred_at = ?
                WHERE id = ?
            """;

    @Mock
    private UtcClock clock;

    @InjectMocks
    private StreamStatusErrorPersistence streamStatusErrorPersistence;

    @Test
    public void shouldLockRowForUpdate() throws Exception {

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";
        final long positionInStream = 2323L;

        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(connection.prepareStatement(SELECT_FOR_UPDATE_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getLong("position")).thenReturn(positionInStream);

        assertThat(streamStatusErrorPersistence.lockStreamForUpdate(streamId, source, component, connection), is(positionInStream));

        final InOrder inOrder = inOrder(preparedStatement, resultSet);

        inOrder.verify(preparedStatement).setObject(1, streamId);
        inOrder.verify(preparedStatement).setString(2, source);
        inOrder.verify(preparedStatement).setString(3, component);
        inOrder.verify(preparedStatement).executeQuery();
        inOrder.verify(resultSet).close();
        inOrder.verify(preparedStatement).close();

        verify(connection, never()).close();
    }

    @Test
    public void shouldThrowStreamErrorHandlingExceptionIfLockRowForUpdateFails() throws Exception {

        final UUID streamId = fromString("4a51e597-c019-4931-9472-5e17cc5bb839");
        final String source = "some-source";
        final String component = "some-component";

        final SQLException sqlException = new SQLException("Ooops");
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(connection.prepareStatement(SELECT_FOR_UPDATE_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenThrow(sqlException);

        final StreamErrorHandlingException streamErrorHandlingException = assertThrows(
                StreamErrorHandlingException.class,
                () -> streamStatusErrorPersistence.lockStreamForUpdate(streamId, source, component, connection));

        assertThat(streamErrorHandlingException.getCause(), is(sqlException));
        assertThat(streamErrorHandlingException.getMessage(), is("Failed to lock row in stream_status table: streamId '4a51e597-c019-4931-9472-5e17cc5bb839', source 'some-source', component 'some-component'"));

        verify(preparedStatement).close();
        verify(resultSet).close();
        verify(connection, never()).close();
    }

    @Test
    public void shouldThrowStreamNotFoundExceptionIfNoStreamFoundInStreamStatusTable() throws Exception {

        final UUID streamId = fromString("4a51e597-c019-4931-9472-5e17cc5bb839");
        final String source = "some-source";
        final String component = "some-component";

        final SQLException sqlException = new SQLException("Ooops");
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(connection.prepareStatement(SELECT_FOR_UPDATE_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        final StreamNotFoundException streamNotFoundException = assertThrows(
                StreamNotFoundException.class,
                () -> streamStatusErrorPersistence.lockStreamForUpdate(streamId, source, component, connection));

        assertThat(streamNotFoundException.getMessage(), is("Failed to lock row in stream_status table. Stream with stream_id '4a51e597-c019-4931-9472-5e17cc5bb839', source 'some-source' and component 'some-component' does not exist"));

        verify(preparedStatement).close();
        verify(resultSet).close();
        verify(connection, never()).close();
    }

    @Test
    public void shouldUpdateStreamErrorOccurredAt() throws Exception {

        final UUID streamErrorId = randomUUID();
        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";
        final ZonedDateTime occurredAt = ZonedDateTime.now();

        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(clock.now()).thenReturn(occurredAt);
        when(connection.prepareStatement(UPDATE_STREAM_ERROR_OCCURRED_AT_SQL)).thenReturn(preparedStatement);

        streamStatusErrorPersistence.updateStreamErrorOccurredAt(streamErrorId, streamId, source, component, connection);

        final InOrder inOrder = inOrder(preparedStatement);
        inOrder.verify(preparedStatement).setTimestamp(1, toSqlTimestamp(occurredAt));
        inOrder.verify(preparedStatement).setObject(2, streamErrorId);
        inOrder.verify(preparedStatement).executeUpdate();
        inOrder.verify(preparedStatement).close();

        verify(connection, never()).close();
    }

    @Test
    public void shouldThrowStreamErrorHandlingExceptionWhenUpdateStreamErrorOccurredAtFails() throws Exception {

        final UUID streamErrorId = fromString("4a51e597-c019-4931-9472-5e17cc5bb839");
        final UUID streamId = fromString("b3f1e597-c019-4931-9472-5e17cc5bb123");
        final String source = "some-source";
        final String component = "some-component";
        final ZonedDateTime occurredAt = ZonedDateTime.now();
        final SQLException sqlException = new SQLException("Database error");

        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(clock.now()).thenReturn(occurredAt);
        when(connection.prepareStatement(UPDATE_STREAM_ERROR_OCCURRED_AT_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenThrow(sqlException);

        final StreamErrorHandlingException streamErrorHandlingException = assertThrows(
                StreamErrorHandlingException.class,
                () -> streamStatusErrorPersistence.updateStreamErrorOccurredAt(streamErrorId, streamId, source, component, connection));

        assertThat(streamErrorHandlingException.getCause(), is(sqlException));
        assertThat(streamErrorHandlingException.getMessage(), is("Failed to update stream_error occurred_at. streamErrorId: '4a51e597-c019-4931-9472-5e17cc5bb839', streamId: 'b3f1e597-c019-4931-9472-5e17cc5bb123', source: 'some-source', component: 'some-component'"));

        verify(preparedStatement).close();
        verify(connection, never()).close();
    }
}