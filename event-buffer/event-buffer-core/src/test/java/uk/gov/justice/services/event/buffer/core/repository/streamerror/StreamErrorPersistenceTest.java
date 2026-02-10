package uk.gov.justice.services.event.buffer.core.repository.streamerror;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StreamErrorPersistenceTest {

    @Mock
    private StreamErrorHashPersistence streamErrorHashPersistence;

    @Mock
    private StreamErrorOccurrencePersistence streamErrorOccurrencePersistence;

    @InjectMocks
    private StreamErrorPersistence streamErrorPersistence;

    @Test
    public void shouldInsertStreamErrorAndUpsertStreamErrorHash() throws Exception {

        final StreamErrorOccurrence streamErrorOccurrence = mock(StreamErrorOccurrence.class);
        final StreamErrorHash streamErrorHash = mock(StreamErrorHash.class);
        final Connection connection = mock(Connection.class);

        when(streamErrorOccurrencePersistence.insert(streamErrorOccurrence, connection)).thenReturn(1);

        final boolean atLeastOneEventProcessed = streamErrorPersistence.save(new StreamError(streamErrorOccurrence, streamErrorHash), connection);
        assertThat(atLeastOneEventProcessed, is(true));

        final InOrder inOrder = inOrder(streamErrorHashPersistence, streamErrorOccurrencePersistence, connection);

        inOrder.verify(streamErrorHashPersistence).upsert(streamErrorHash, connection);
        inOrder.verify(streamErrorOccurrencePersistence).insert(streamErrorOccurrence, connection);

        verify(connection, never()).close();
    }

    @Test
    public void shouldReturnFalseIfInsertIntoStreamErrorDoesNotUpdateAnyRows() throws Exception {

        final StreamErrorOccurrence streamErrorOccurrence = mock(StreamErrorOccurrence.class);
        final StreamErrorHash streamErrorHash = mock(StreamErrorHash.class);
        final Connection connection = mock(Connection.class);

        when(streamErrorOccurrencePersistence.insert(streamErrorOccurrence, connection)).thenReturn(0);

        final boolean atLeastOneEventProcessed = streamErrorPersistence.save(new StreamError(streamErrorOccurrence, streamErrorHash), connection);
        assertThat(atLeastOneEventProcessed, is(false));

        final InOrder inOrder = inOrder(streamErrorHashPersistence, streamErrorOccurrencePersistence, connection);

        inOrder.verify(streamErrorHashPersistence).upsert(streamErrorHash, connection);
        inOrder.verify(streamErrorOccurrencePersistence).insert(streamErrorOccurrence, connection);

        verify(connection, never()).close();
    }

    @Test
    public void shouldThrowStreamErrorHandlingExceptionOnFailureToSave() throws Exception {

        final SQLException sqlException = new SQLException("Shiver me timbers");

        final StreamErrorOccurrence streamErrorOccurrence = mock(StreamErrorOccurrence.class);
        final StreamErrorHash streamErrorHash = mock(StreamErrorHash.class);
        final Connection connection = mock(Connection.class);

        when(streamErrorOccurrence.toString()).thenReturn(StreamErrorOccurrence.class.getSimpleName());
        when(streamErrorHash.toString()).thenReturn(StreamErrorHash.class.getSimpleName());

        doThrow(sqlException).when(streamErrorOccurrencePersistence).insert(streamErrorOccurrence, connection);

        final StreamErrorHandlingException streamErrorHandlingException = assertThrows(
                StreamErrorHandlingException.class,
                () -> streamErrorPersistence.save(new StreamError(streamErrorOccurrence, streamErrorHash) , connection));

        assertThat(streamErrorHandlingException.getCause(), is(sqlException));
        assertThat(streamErrorHandlingException.getMessage(), is("Failed to save StreamError: StreamError[streamErrorOccurrence=StreamErrorOccurrence, streamErrorHash=StreamErrorHash]"));

        verify(connection, never()).close();
    }

    @Test
    public void shouldFindByErrorIdStreamErrorId() throws Exception {

        final UUID streamErrorId = randomUUID();
        final String hash = "some-hash";

        final Connection connection = mock(Connection.class);
        final StreamErrorOccurrence streamErrorOccurrence = mock(StreamErrorOccurrence.class);
        final StreamErrorHash streamErrorHash = mock(StreamErrorHash.class);

        when(streamErrorOccurrencePersistence.findById(streamErrorId, connection)).thenReturn(of(streamErrorOccurrence));
        when(streamErrorOccurrence.hash()).thenReturn(hash);
        when(streamErrorHashPersistence.findByHash(streamErrorOccurrence.hash(), connection)).thenReturn(of(streamErrorHash));

        final Optional<StreamError> streamErrorOptional = streamErrorPersistence.findByErrorId(streamErrorId, connection);

        assertThat(streamErrorOptional.isPresent(), is(true));
        assertThat(streamErrorOptional.get().streamErrorOccurrence(), is(streamErrorOccurrence));
        assertThat(streamErrorOptional.get().streamErrorHash(), is(streamErrorHash));

        verify(connection, never()).close();
    }

    @Test
    public void shouldReturnEmptyIfStreamErrorHashNotFound() throws Exception {

        final UUID streamErrorId = randomUUID();
        final String hash = "some-hash";

        final Connection connection = mock(Connection.class);
        final StreamErrorOccurrence streamErrorOccurrence = mock(StreamErrorOccurrence.class);

        when(streamErrorOccurrencePersistence.findById(streamErrorId, connection)).thenReturn(of(streamErrorOccurrence));
        when(streamErrorOccurrence.hash()).thenReturn(hash);
        when(streamErrorHashPersistence.findByHash(streamErrorOccurrence.hash(), connection)).thenReturn(empty());

        assertThat(streamErrorPersistence.findByErrorId(streamErrorId, connection), is(empty()));

        verify(connection, never()).close();
    }

    @Test
    public void shouldReturnEmptyIfStreamErrorDetailsNotFound() throws Exception {

        final UUID streamErrorId = randomUUID();
        final Connection connection = mock(Connection.class);

        when(streamErrorOccurrencePersistence.findById(streamErrorId, connection)).thenReturn(empty());

        assertThat(streamErrorPersistence.findByErrorId(streamErrorId, connection), is(empty()));

        verify(connection, never()).close();
    }

    @Test
    public void shouldThrowStreamErrorHandlingExceptionIfFindingByStreamErrorIdThrowsSqlexception() throws Exception {

        final SQLException sqlException = new SQLException();
        final UUID streamErrorId = fromString("f4ab7943-6220-45a0-8da9-200f5e877b67");
        final String hash = "some-hash";

        final Connection connection = mock(Connection.class);
        final StreamErrorOccurrence streamErrorOccurrence = mock(StreamErrorOccurrence.class);

        when(streamErrorOccurrencePersistence.findById(streamErrorId, connection)).thenReturn(of(streamErrorOccurrence));
        when(streamErrorOccurrence.hash()).thenReturn(hash);
        when(streamErrorHashPersistence.findByHash(streamErrorOccurrence.hash(), connection)).thenThrow(sqlException);

        final StreamErrorHandlingException streamErrorHandlingException = assertThrows(
                StreamErrorHandlingException.class,
                () -> streamErrorPersistence.findByErrorId(streamErrorId, connection));

        assertThat(streamErrorHandlingException.getCause(), is(sqlException));
        assertThat(streamErrorHandlingException.getMessage(), is("Failed find StreamError by streamErrorId: 'f4ab7943-6220-45a0-8da9-200f5e877b67'"));

        verify(connection, never()).close();
    }

    @Test
    public void shouldRemoveErrorForStream() throws Exception {

        final UUID streamId = randomUUID();
        final UUID streamErrorId = randomUUID();
        final String source = "some-source";
        final String componentName = "some-component";
        final String hash = "some-hash";

        final Connection connection = mock(Connection.class);

        when(streamErrorOccurrencePersistence.deleteErrorAndGetHash(streamErrorId, connection)).thenReturn(hash);
        when(streamErrorOccurrencePersistence.noErrorsExistFor(hash, connection)).thenReturn(false);

        streamErrorPersistence.removeErrorForStream(streamErrorId, streamId, source, componentName, connection);

        verify(connection, never()).close();
        verify(streamErrorHashPersistence, never()).deleteHash(hash, connection);
    }

    @Test
    public void shouldAlsoRemoveHashWhenRemovingErrorForStreamIfNoErrorsExistWithThatHash() throws Exception {

        final UUID streamId = randomUUID();
        final UUID streamErrorId = randomUUID();
        final String source = "some-source";
        final String componentName = "some-component";
        final String hash = "some-hash";

        final Connection connection = mock(Connection.class);

        when(streamErrorOccurrencePersistence.deleteErrorAndGetHash(streamErrorId, connection)).thenReturn(hash);
        when(streamErrorOccurrencePersistence.noErrorsExistFor(hash, connection)).thenReturn(true);

        streamErrorPersistence.removeErrorForStream(streamErrorId, streamId, source, componentName, connection);

        verify(streamErrorHashPersistence).deleteHash(hash, connection);

        verify(connection, never()).close();
    }

    @Test
    public void shouldThrowStreamErrorHandlingExceptionIfRemovingErrorForStreamFails() throws Exception {

        final SQLException sqlException = new SQLException("Bunnies");
        final UUID streamId = fromString("ad6b76f1-96b7-423b-a2d0-4a922236c2ad");
        final UUID streamErrorId = randomUUID();
        final String source = "some-source";
        final String componentName = "some-component";
        final String hash = "some-hash";

        final Connection connection = mock(Connection.class);

        when(streamErrorOccurrencePersistence.deleteErrorAndGetHash(streamErrorId, connection)).thenReturn(hash);
        doThrow(sqlException).when(streamErrorOccurrencePersistence).noErrorsExistFor(hash, connection);

        final StreamErrorHandlingException streamErrorHandlingException = assertThrows(
                StreamErrorHandlingException.class,
                () -> streamErrorPersistence.removeErrorForStream(streamErrorId, streamId, source, componentName, connection));

        assertThat(streamErrorHandlingException.getCause(), is(sqlException));
        assertThat(streamErrorHandlingException.getMessage(), is("Failed to remove error for stream. streamId: 'ad6b76f1-96b7-423b-a2d0-4a922236c2ad', source: 'some-source, component: 'some-component'"));

        verify(connection, never()).close();
    }

    @Test
    public void shouldFindAllStreamErrorsByStreamId() throws Exception {

        final UUID streamId = randomUUID();
        final String hash = "some-hash";

        final StreamErrorOccurrence streamErrorOccurrence_1 = mock(StreamErrorOccurrence.class);
        final StreamErrorOccurrence streamErrorOccurrence_2 = mock(StreamErrorOccurrence.class);
        final StreamErrorHash streamErrorHash = mock(StreamErrorHash.class);
        final Connection connection = mock(Connection.class);

        when(streamErrorOccurrencePersistence.findByStreamId(streamId, connection)).thenReturn(List.of(streamErrorOccurrence_1, streamErrorOccurrence_2));

        when(streamErrorOccurrence_1.hash()).thenReturn(hash);
        when(streamErrorOccurrence_2.hash()).thenReturn(hash);
        when(streamErrorHashPersistence.findByHash(hash, connection)).thenReturn(of(streamErrorHash));

        final List<StreamError> streamErrors = streamErrorPersistence.findAllByStreamId(streamId, connection);

        assertThat(streamErrors.size(), is(2));
        assertThat(streamErrors.get(0).streamErrorOccurrence(), is(streamErrorOccurrence_1));
        assertThat(streamErrors.get(0).streamErrorHash(), is(streamErrorHash));
        assertThat(streamErrors.get(1).streamErrorOccurrence(), is(streamErrorOccurrence_2));
        assertThat(streamErrors.get(1).streamErrorHash(), is(streamErrorHash));
        verify(connection, never()).close();
    }

    @Test
    public void shouldThrowStreamErrorHandlingExceptionIfFindAllByStreamIdFails() throws Exception {

        final UUID streamId = fromString("30b0c79b-af2f-4826-a63f-503e27c8d932");
        final SQLException sqlException = new SQLException("Ooops");

        final Connection connection = mock(Connection.class);

        when(streamErrorOccurrencePersistence.findByStreamId(streamId, connection)).thenThrow(sqlException);

        final StreamErrorHandlingException streamErrorHandlingException = assertThrows(
                StreamErrorHandlingException.class,
                () -> streamErrorPersistence.findAllByStreamId(streamId, connection));

        assertThat(streamErrorHandlingException.getCause(), is(sqlException));
        assertThat(streamErrorHandlingException.getMessage(), is("Failed find List of StreamErrors by streamId: '30b0c79b-af2f-4826-a63f-503e27c8d932'"));

        verify(connection, never()).close();
    }

    @Test
    public void shouldThrowStreamErrorHandlingExceptionIfNoMatchingStreamErrorHashFound() throws Exception {

        final UUID streamId = randomUUID();
        final String hash = "some-hash";

        final StreamErrorOccurrence streamErrorOccurrence_1 = mock(StreamErrorOccurrence.class);
        final Connection connection = mock(Connection.class);

        when(streamErrorOccurrencePersistence.findByStreamId(streamId, connection)).thenReturn(List.of(streamErrorOccurrence_1));

        when(streamErrorOccurrence_1.hash()).thenReturn(hash);
        when(streamErrorHashPersistence.findByHash(hash, connection)).thenReturn(empty());

        final StreamErrorHandlingException streamErrorHandlingException = assertThrows(
                StreamErrorHandlingException.class,
                () -> streamErrorPersistence.findAllByStreamId(streamId, connection));

        assertThat(streamErrorHandlingException.getMessage(), is("No stream_error found for hash 'some-hash' yet hash exists in stream_error table"));

        verify(connection, never()).close();
    }
}