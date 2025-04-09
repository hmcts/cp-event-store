package uk.gov.justice.services.event.sourcing.subscription.error;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.core.interceptor.InterceptorChainProcessor;
import uk.gov.justice.services.core.interceptor.InterceptorContext;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorHandlingException;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamStatusErrorPersistence;
import uk.gov.justice.services.event.sourcing.subscription.manager.cdi.InterceptorContextProvider;
import uk.gov.justice.services.eventsourcing.source.api.streams.MissingStreamIdException;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.Metadata;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.UUID;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class SubscriptionEventProcessorTest {

    @Mock
    private InterceptorContextProvider interceptorContextProvider;

    @Mock
    private InterceptorChainProcessor interceptorChainProcessor;

    @Mock
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    @Mock
    private StreamProcessingFailureHandler streamProcessingFailureHandler;

    @Mock
    private StreamStatusErrorPersistence streamStatusErrorPersistence;

    @InjectMocks
    private SubscriptionEventProcessor subscriptionEventProcessor;

    @Test
    public void shouldSetSavepointThenProcessEventThenReleaseSavePoint() throws Exception {

        final String componentName = "EVENT_LISTENER";
        final String eventName = "some-event-name";
        final UUID eventId = randomUUID();
        final UUID streamId = randomUUID();
        final String source = "some-source";

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);
        final InterceptorContext interceptorContext = mock(InterceptorContext.class);
        final Savepoint savepoint = mock(Savepoint.class);

        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(eventName);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.streamId()).thenReturn(of(streamId));
        when(metadata.source()).thenReturn(of(source));
        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.setSavepoint()).thenReturn(savepoint);
        when(interceptorContextProvider.getInterceptorContext(eventJsonEnvelope)).thenReturn(interceptorContext);

        subscriptionEventProcessor.processSingleEvent(eventJsonEnvelope, componentName);

        final InOrder inOrder = inOrder(
                streamStatusErrorPersistence,
                interceptorChainProcessor,
                streamProcessingFailureHandler,
                connection,
                savepoint);

        inOrder.verify(streamStatusErrorPersistence).lockStreamForUpdate(streamId, source, componentName, connection);
        inOrder.verify(connection).setSavepoint();
        inOrder.verify(interceptorChainProcessor).process(interceptorContext);
        inOrder.verify(streamProcessingFailureHandler).onStreamProcessingSucceeded(eventJsonEnvelope, componentName);
        inOrder.verify(connection).releaseSavepoint(savepoint);
        inOrder.verify(connection).close();

        verify(connection, never()).rollback(savepoint);
        verify(streamProcessingFailureHandler, never()).onStreamProcessingFailure(eq(eventJsonEnvelope), any(Exception.class), eq(componentName));
    }

    @Test
    public void shouldThrowMissingStreamIdExceptionIfNoStreamIdFoundInJsonEnvelope() throws Exception {

        final String componentName = "EVENT_LISTENER";
        final String eventName = "some-event-name";
        final UUID eventId = fromString("8c2ba9d6-63b6-40f5-8565-16a3637720bb");

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);

        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(eventName);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.streamId()).thenReturn(empty());
        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);

        final MissingStreamIdException missingStreamIdException = assertThrows(
                MissingStreamIdException.class,
                () -> subscriptionEventProcessor.processSingleEvent(eventJsonEnvelope, componentName));

        assertThat(missingStreamIdException.getMessage(), is("No streamId found in event: name 'some-event-name', eventId '8c2ba9d6-63b6-40f5-8565-16a3637720bb'"));
    }

    @Test
    public void shouldThrowMissingSourceExceptionIfNoSourceFoundInJsonEnvelope() throws Exception {

        final String componentName = "EVENT_LISTENER";
        final String eventName = "some-event-name";
        final UUID eventId = fromString("8c2ba9d6-63b6-40f5-8565-16a3637720bb");
        final UUID streamId = randomUUID();

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);

        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(eventName);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.streamId()).thenReturn(of(streamId));
        when(metadata.source()).thenReturn(empty());
        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);

        final MissingSourceException missingSourceException = assertThrows(
                MissingSourceException.class,
                () -> subscriptionEventProcessor.processSingleEvent(eventJsonEnvelope, componentName));

        assertThat(missingSourceException.getMessage(), is("No source found in event: name 'some-event-name', eventId '8c2ba9d6-63b6-40f5-8565-16a3637720bb'"));
    }

    @Test
    public void shouldRollBackToSavepointIfProcessingEventThrowsAnException() throws Exception {

        final NullPointerException nullPointerException = new NullPointerException();

        final String componentName = "EVENT_LISTENER";
        final String eventName = "some-event-name";
        final UUID eventId = fromString("5f172bf9-4eca-4677-ba24-e3dda82b4e92");
        final UUID streamId = fromString("e9f133f7-b546-4b4f-b055-ce1c45323c90");
        final String source = "some-source";

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final Savepoint savepoint = mock(Savepoint.class);
        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);
        final InterceptorContext interceptorContext = mock(InterceptorContext.class);

        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(eventName);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.streamId()).thenReturn(of(streamId));
        when(metadata.source()).thenReturn(of(source));
        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.setSavepoint()).thenReturn(savepoint);
        when(interceptorContextProvider.getInterceptorContext(eventJsonEnvelope)).thenReturn(interceptorContext);
        doThrow(nullPointerException).when(interceptorChainProcessor).process(interceptorContext);

        final StreamProcessingException streamProcessingException = assertThrows(
                StreamProcessingException.class,
                () -> subscriptionEventProcessor.processSingleEvent(eventJsonEnvelope, componentName));

        assertThat(streamProcessingException.getCause(), is(nullPointerException));
        assertThat(streamProcessingException.getMessage(), is("Failed to process event. name: 'some-event-name', eventId: '5f172bf9-4eca-4677-ba24-e3dda82b4e92', streamId: 'e9f133f7-b546-4b4f-b055-ce1c45323c90'"));

        final InOrder inOrder = inOrder(
                streamStatusErrorPersistence,
                interceptorChainProcessor,
                streamProcessingFailureHandler,
                connection,
                savepoint);

        inOrder.verify(streamStatusErrorPersistence).lockStreamForUpdate(streamId, source, componentName, connection);
        inOrder.verify(connection).setSavepoint();
        inOrder.verify(interceptorChainProcessor).process(interceptorContext);
        inOrder.verify(connection).rollback(savepoint);
        inOrder.verify(streamProcessingFailureHandler).onStreamProcessingFailure(eventJsonEnvelope, nullPointerException, componentName);
        inOrder.verify(connection).close();

        verify(connection, never()).releaseSavepoint(savepoint);
        verify(streamProcessingFailureHandler, never()).onStreamProcessingSucceeded(eventJsonEnvelope, componentName);
    }

    @Test
    public void shouldThrowStreamErrorHandlingExceptionIfGettingDatabaseConnectionFails() throws Exception {

        final String componentName = "EVENT_LISTENER";

        final SQLException sqlException = new SQLException("Ooops");

        final DataSource dataSource = mock(DataSource.class);
        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenThrow(sqlException);

        final StreamErrorHandlingException streamErrorHandlingException = assertThrows(
                StreamErrorHandlingException.class,
                () -> subscriptionEventProcessor.processSingleEvent(eventJsonEnvelope, componentName));

        assertThat(streamErrorHandlingException.getCause(), is(sqlException));
        assertThat(streamErrorHandlingException.getMessage(), is("Failed to get database connection to viewstore"));
    }
}