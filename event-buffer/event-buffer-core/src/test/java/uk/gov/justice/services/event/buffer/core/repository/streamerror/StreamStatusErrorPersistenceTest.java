package uk.gov.justice.services.event.buffer.core.repository.streamerror;

import static java.util.Optional.empty;
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

import uk.gov.justice.services.common.util.UtcClock;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
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

    private static final String UPDATE_STREAM_UPDATED_AT_IF_ERROR_IS_SAME = """
                UPDATE stream_status
                SET updated_at = ?
                WHERE stream_id = ?
                AND source = ?
                AND component = ?
                AND position = ?
                AND updated_at  = ?
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
    public void shouldUpdateStreamStatusUpdatedAtForSameError() throws Exception {

        final UUID streamId = randomUUID();
        final UUID streamErrorId = randomUUID();
        final String source = "some-source";
        final String componentName = "some-component";
        final Timestamp previousUpdateAtTimestamp = new Timestamp(System.currentTimeMillis() - 1000);
        final ZonedDateTime updatedAt = ZonedDateTime.now();
        final int expectedRowsUpdated = 1;
        final long lastStreamPosition = 122L;


        final StreamErrorDetails streamErrorDetails = new StreamErrorDetails(
                streamErrorId,
                "some-hash",
                "exception-message",
                empty(),
                "event-name",
                randomUUID(),
                streamId,
                123L,
                ZonedDateTime.now(),
                "stack-trace",
                componentName,
                source
        );

        final StreamError streamError = new StreamError(streamErrorDetails, null);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(clock.now()).thenReturn(updatedAt);
        when(connection.prepareStatement(UPDATE_STREAM_UPDATED_AT_IF_ERROR_IS_SAME)).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenReturn(expectedRowsUpdated);

        final int actualRowsUpdated = streamStatusErrorPersistence.updateStreamStatusUpdatedAtForSameError(
                streamError,
                lastStreamPosition,
                previousUpdateAtTimestamp,
                connection
        );

        assertThat(actualRowsUpdated, is(expectedRowsUpdated));

        final InOrder inOrder = inOrder(preparedStatement);
        inOrder.verify(preparedStatement).setObject(1, Timestamp.from(updatedAt.toInstant()));
        inOrder.verify(preparedStatement).setObject(2, streamId);
        inOrder.verify(preparedStatement).setString(3, source);
        inOrder.verify(preparedStatement).setString(4, componentName);
        inOrder.verify(preparedStatement).setObject(5, lastStreamPosition);
        inOrder.verify(preparedStatement).setObject(6, previousUpdateAtTimestamp);
        inOrder.verify(preparedStatement).executeUpdate();
        inOrder.verify(preparedStatement).close();

        verify(connection, never()).close();
    }

    @Test
    public void shouldThrowStreamErrorHandlingExceptionWhenUpdateStreamStatusUpdatedAtForSameErrorFails() throws Exception {

        final UUID streamId = fromString("4a51e597-c019-4931-9472-5e17cc5bb839");
        final UUID streamErrorId = randomUUID();
        final String source = "some-source";
        final String componentName = "some-component";
        final Timestamp previousUpdateAtTimestamp = new Timestamp(System.currentTimeMillis() - 1000);
        final ZonedDateTime updatedAt = ZonedDateTime.now();
        final SQLException sqlException = new SQLException("Database error");
        final long lastStreamPosition = 122L;


        final StreamErrorDetails streamErrorDetails = new StreamErrorDetails(
                streamErrorId,
                "some-hash",
                "exception-message",
                empty(),
                "event-name",
                randomUUID(),
                streamId,
                123L,
                ZonedDateTime.now(),
                "stack-trace",
                componentName,
                source
        );

        final StreamError streamError = new StreamError(streamErrorDetails, null);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(clock.now()).thenReturn(updatedAt);
        when(connection.prepareStatement(UPDATE_STREAM_UPDATED_AT_IF_ERROR_IS_SAME)).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenThrow(sqlException);

        final StreamErrorHandlingException streamErrorHandlingException = assertThrows(
                StreamErrorHandlingException.class,
                () -> streamStatusErrorPersistence.updateStreamStatusUpdatedAtForSameError(
                        streamError,
                        lastStreamPosition,
                        previousUpdateAtTimestamp,
                        connection
                ));

        assertThat(streamErrorHandlingException.getCause(), is(sqlException));
        assertThat(streamErrorHandlingException.getMessage(), is("Failed to update Stream Status updated at. streamId: '4a51e597-c019-4931-9472-5e17cc5bb839', source: 'some-source, component: 'some-component'"));

        verify(preparedStatement).close();
        verify(connection, never()).close();
    }
}