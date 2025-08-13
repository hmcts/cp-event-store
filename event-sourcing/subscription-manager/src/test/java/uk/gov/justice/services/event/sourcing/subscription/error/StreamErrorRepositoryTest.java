package uk.gov.justice.services.event.sourcing.subscription.error;

import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.slf4j.Logger;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamError;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorDetails;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorHandlingException;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorPersistence;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamStatusErrorPersistence;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StreamErrorRepositoryTest {

    @Mock
    private StreamErrorPersistence streamErrorPersistence;

    @Mock
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    @Mock
    private StreamStatusErrorPersistence streamStatusErrorPersistence;

    @Mock
    private Logger logger;

    @InjectMocks
    private StreamErrorRepository streamErrorRepository;

    @Test
    public void shouldMarkStreamAsErrored() throws Exception {

        final UUID streamErrorId = randomUUID();
        final UUID streamId = randomUUID();
        final Long positionInStream = 98239847L;
        final Long currentPosition = 98239846L;
        final String componentName = "SOME_COMPONENT";
        final String source = "some-source";
        final StreamError streamError = mock(StreamError.class);
        final StreamErrorDetails streamErrorDetails = mock(StreamErrorDetails.class);

        final DataSource viewStoreDataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);
        when(viewStoreDataSource.getConnection()).thenReturn(connection);

        when(streamStatusErrorPersistence.lockStreamForUpdate(streamId, source, componentName, connection)).thenReturn(currentPosition);

        when(streamErrorPersistence.save(streamError, connection)).thenReturn(true);

        when(streamError.streamErrorDetails()).thenReturn(streamErrorDetails);
        when(streamErrorDetails.id()).thenReturn(streamErrorId);
        when(streamErrorDetails.streamId()).thenReturn(streamId);
        when(streamErrorDetails.positionInStream()).thenReturn(positionInStream);
        when(streamErrorDetails.componentName()).thenReturn(componentName);
        when(streamErrorDetails.source()).thenReturn(source);

        streamErrorRepository.markStreamAsErrored(streamError, currentPosition);

        final InOrder inOrder = inOrder(streamStatusErrorPersistence, streamErrorPersistence, connection);
        inOrder.verify(streamErrorPersistence).save(streamError, connection);
        inOrder.verify(streamStatusErrorPersistence).markStreamAsErrored(
                streamId,
                streamErrorId,
                positionInStream,
                componentName,
                source,
                connection);
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldNotMarkStreamStatusAsErroredIfSavingStreamErrorDoesNotUpdateAnyRows() throws Exception {

        final StreamError streamError = mock(StreamError.class);

        final UUID streamErrorId = randomUUID();
        final UUID streamId = randomUUID();
        final Long positionInStream = 98239847L;
        final Long currentPosition = 98239846L;
        final String componentName = "SOME_COMPONENT";
        final String source = "some-source";
        final StreamErrorDetails streamErrorDetails = mock(StreamErrorDetails.class);


        final DataSource viewStoreDataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);
        when(viewStoreDataSource.getConnection()).thenReturn(connection);

        when(streamError.streamErrorDetails()).thenReturn(streamErrorDetails);
        when(streamErrorDetails.id()).thenReturn(streamErrorId);
        when(streamErrorDetails.streamId()).thenReturn(streamId);
        when(streamErrorDetails.positionInStream()).thenReturn(positionInStream);
        when(streamErrorDetails.componentName()).thenReturn(componentName);
        when(streamErrorDetails.source()).thenReturn(source);

        when(streamStatusErrorPersistence.lockStreamForUpdate(streamId, source, componentName, connection)).thenReturn(currentPosition);


        when(streamErrorPersistence.save(streamError, connection)).thenReturn(false);

        streamErrorRepository.markStreamAsErrored(streamError, currentPosition);

        verify(streamErrorPersistence).save(streamError, connection);
        verify(streamStatusErrorPersistence).lockStreamForUpdate(streamId, source, componentName, connection);
        verifyNoMoreInteractions(streamErrorPersistence, streamStatusErrorPersistence);
        verify(connection).close();
    }

    @Test
    public void shouldThrowStreamErrorHandlingExceptionIfGettingConnectionFailsWhenSavingStreamErrorAndHash() throws Exception {

        final StreamError streamError = mock(StreamError.class);
        final SQLException sqlException = new SQLException("Oops");

        final DataSource viewStoreDataSource = mock(DataSource.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);
        when(viewStoreDataSource.getConnection()).thenThrow(sqlException);


        final StreamErrorHandlingException streamErrorHandlingException = assertThrows(
                StreamErrorHandlingException.class,
                () -> streamErrorRepository.markStreamAsErrored(streamError, 0L));

        assertThat(streamErrorHandlingException.getCause(), is(sqlException));
        assertThat(streamErrorHandlingException.getMessage(), is("Failed to get connection to view-store"));
    }

    @Test
    public void shouldMarkStreamAsFixed() throws Exception {

        final UUID streamId = randomUUID();
        final UUID streamErrorId = randomUUID();
        final String componentName = "SOME_COMPONENT";
        final String source = "some-source";

        final DataSource viewStoreDataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);
        when(viewStoreDataSource.getConnection()).thenReturn(connection);

        streamErrorRepository.markStreamAsFixed(streamErrorId, streamId, source, componentName);

        final InOrder inOrder = inOrder(streamStatusErrorPersistence, streamErrorPersistence, connection);
        inOrder.verify(streamStatusErrorPersistence).unmarkStreamStatusAsErrored(streamId, source, componentName, connection);
        inOrder.verify(streamErrorPersistence).removeErrorForStream(streamErrorId, streamId, source, componentName, connection);
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldThrowStreamErrorHandlingExceptionIfGettingConnectionFailsWhenMarkingStreamAsFixed() throws Exception {

        final UUID streamId = randomUUID();
        final UUID streamErrorId = randomUUID();
        final String componentName = "SOME_COMPONENT";
        final String source = "some-source";
        final SQLException sqlException = new SQLException("Ooops");

        final DataSource viewStoreDataSource = mock(DataSource.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);
        when(viewStoreDataSource.getConnection()).thenThrow(sqlException);

        final StreamErrorHandlingException streamErrorHandlingException = assertThrows(
                StreamErrorHandlingException.class,
                () -> streamErrorRepository.markStreamAsFixed(streamErrorId, streamId, source, componentName));

        assertThat(streamErrorHandlingException.getCause(), is(sqlException));
        assertThat(streamErrorHandlingException.getMessage(), is("Failed to get connection to view-store"));
    }

    @Test
    public void shouldFindAllByStreamId() throws Exception {

        final UUID streamId = randomUUID();
        final DataSource viewStoreDataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final StreamError streamError_1 = mock(StreamError.class);
        final StreamError streamError_2 = mock(StreamError.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);
        when(viewStoreDataSource.getConnection()).thenReturn(connection);
        when(streamErrorPersistence.findAllByStreamId(streamId, connection)).thenReturn(List.of(streamError_1, streamError_2));

        final List<StreamError> streamErrors = streamErrorRepository.findAllByStreamId(streamId);

        assertThat(streamErrors.size(), is(2));
        assertThat(streamErrors.get(0), is(streamError_1));
        assertThat(streamErrors.get(1), is(streamError_2));

        verify(connection).close();
    }

    @Test
    public void shouldThrowStreamErrorHandlingExceptionIfFindAllByStreamIdFails() throws Exception {

        final UUID streamId = randomUUID();
        final DataSource viewStoreDataSource = mock(DataSource.class);
        final SQLException sqlException = new SQLException("Ooops");

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);
        when(viewStoreDataSource.getConnection()).thenThrow(sqlException);

        final StreamErrorHandlingException streamErrorHandlingException = assertThrows(
                StreamErrorHandlingException.class,
                () -> streamErrorRepository.findAllByStreamId(streamId));

        assertThat(streamErrorHandlingException.getCause(), is(sqlException));
        assertThat(streamErrorHandlingException.getMessage(), is("Failed to get connection to view-store"));
    }

    @Test
    public void shouldCloseConnectionOnErrorWhenFindingByStreamId() throws Exception {

        final UUID streamId = randomUUID();
        final DataSource viewStoreDataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final StreamErrorHandlingException streamErrorHandlingException = new StreamErrorHandlingException("Ooops");

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);
        when(viewStoreDataSource.getConnection()).thenReturn(connection);
        when(streamErrorPersistence.findAllByStreamId(streamId, connection)).thenThrow(streamErrorHandlingException);

        final StreamErrorHandlingException thrownStreamErrorHandlingException = assertThrows(
                StreamErrorHandlingException.class,
                () -> streamErrorRepository.findAllByStreamId(streamId));

        assertThat(thrownStreamErrorHandlingException, is(sameInstance(streamErrorHandlingException)));

        verify(connection).close();
    }

    @Test
    public void shouldFindByErrorId() throws Exception {

        final UUID errorId = randomUUID();
        final DataSource viewStoreDataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final Optional<StreamError> streamError = of(mock(StreamError.class));

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);
        when(viewStoreDataSource.getConnection()).thenReturn(connection);
        when(streamErrorPersistence.findByErrorId(errorId, connection)).thenReturn(streamError);

        assertThat(streamErrorRepository.findByErrorId(errorId), is(streamError));

        verify(connection).close();
    }

    @Test
    public void shouldThrowStreamErrorHandlingExceptionIfFindByErrorIdFails() throws Exception {

        final UUID errorId = randomUUID();
        final DataSource viewStoreDataSource = mock(DataSource.class);
        final SQLException sqlException = new SQLException("Ooops");

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);
        when(viewStoreDataSource.getConnection()).thenThrow(sqlException);

        final StreamErrorHandlingException streamErrorHandlingException = assertThrows(
                StreamErrorHandlingException.class,
                () -> streamErrorRepository.findByErrorId(errorId));

        assertThat(streamErrorHandlingException.getCause(), is(sqlException));
        assertThat(streamErrorHandlingException.getMessage(), is("Failed to get connection to view-store"));
    }

    @Test
    public void shouldCloseConnectionIfFindByErrorIdFails() throws Exception {

        final UUID errorId = randomUUID();
        final DataSource viewStoreDataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final Optional<StreamError> streamError = of(mock(StreamError.class));
        final StreamErrorHandlingException streamErrorHandlingException = new StreamErrorHandlingException("Ooops");

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);
        when(viewStoreDataSource.getConnection()).thenReturn(connection);
        when(streamErrorPersistence.findByErrorId(errorId, connection)).thenThrow(streamErrorHandlingException);

        final StreamErrorHandlingException thrownStreamErrorHandlingException = assertThrows(
                StreamErrorHandlingException.class,
                () -> streamErrorRepository.findByErrorId(errorId));

        assertThat(thrownStreamErrorHandlingException, is(sameInstance(streamErrorHandlingException)));

        verify(connection).close();
    }

    @Test
    public void shouldMarkSameErrorHappened() throws Exception {

        final StreamError newStreamError = mock(StreamError.class);
        final Timestamp lastUpdatedAt = new Timestamp(System.currentTimeMillis());
        final long lastStreamPosition = 122L;


        final DataSource viewStoreDataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);
        when(viewStoreDataSource.getConnection()).thenReturn(connection);
        when(streamStatusErrorPersistence.updateStreamStatusUpdatedAtForSameError(newStreamError, lastStreamPosition, lastUpdatedAt, connection)).thenReturn(1);

        streamErrorRepository.markSameErrorHappened(newStreamError, lastStreamPosition, lastUpdatedAt);

        verify(streamStatusErrorPersistence).updateStreamStatusUpdatedAtForSameError(newStreamError, lastStreamPosition, lastUpdatedAt, connection);
        verify(connection).close();
        verifyNoInteractions(logger);
    }

    @Test
    public void shouldLogWarningWhenNoRowsUpdatedInMarkSameErrorHappened() throws Exception {

        final StreamError newStreamError = mock(StreamError.class);
        final StreamErrorDetails streamErrorDetails = mock(StreamErrorDetails.class);
        final Timestamp lastUpdatedAt = new Timestamp(System.currentTimeMillis());
        final UUID streamErrorId = randomUUID();
        final UUID streamId = randomUUID();
        final String componentName = "SOME_COMPONENT";
        final String source = "some-source";
        final long lastStreamPosition = 122L;


        final DataSource viewStoreDataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);
        when(viewStoreDataSource.getConnection()).thenReturn(connection);
        when(newStreamError.streamErrorDetails()).thenReturn(streamErrorDetails);
        when(streamErrorDetails.id()).thenReturn(streamErrorId);
        when(streamErrorDetails.streamId()).thenReturn(streamId);
        when(streamErrorDetails.componentName()).thenReturn(componentName);
        when(streamErrorDetails.source()).thenReturn(source);
        when(streamStatusErrorPersistence.updateStreamStatusUpdatedAtForSameError(newStreamError, lastStreamPosition, lastUpdatedAt, connection)).thenReturn(0);

        streamErrorRepository.markSameErrorHappened(newStreamError, lastStreamPosition, lastUpdatedAt);

        verify(streamStatusErrorPersistence).updateStreamStatusUpdatedAtForSameError(newStreamError, lastStreamPosition, lastUpdatedAt, connection);
        verify(logger).warn("Existing stream status entry is changed by another transaction errorId: {} streamId: {} source {} component {}",
                streamErrorId, streamId, source, componentName);
        verify(connection).close();
    }

    @Test
    public void shouldThrowStreamErrorHandlingExceptionIfGettingConnectionFailsInMarkSameErrorHappened() throws Exception {

        final StreamError newStreamError = mock(StreamError.class);
        final Timestamp lastUpdatedAt = new Timestamp(System.currentTimeMillis());
        final SQLException sqlException = new SQLException("Ooops");
        final long lastStreamPosition = 122L;


        final DataSource viewStoreDataSource = mock(DataSource.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);
        when(viewStoreDataSource.getConnection()).thenThrow(sqlException);

        final StreamErrorHandlingException streamErrorHandlingException = assertThrows(
                StreamErrorHandlingException.class,
                () -> streamErrorRepository.markSameErrorHappened(newStreamError, lastStreamPosition, lastUpdatedAt));

        assertThat(streamErrorHandlingException.getCause(), is(sqlException));
        assertThat(streamErrorHandlingException.getMessage(), is("Failed to get connection to view-store"));
    }
}