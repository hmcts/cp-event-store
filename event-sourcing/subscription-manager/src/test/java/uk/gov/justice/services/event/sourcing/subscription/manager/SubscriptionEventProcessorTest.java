package uk.gov.justice.services.event.sourcing.subscription.manager;

import static java.util.Optional.of;
import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.core.interceptor.InterceptorChainProcessor;
import uk.gov.justice.services.core.interceptor.InterceptorContext;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamStatusErrorPersistence;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamProcessingException;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamProcessingFailureHandler;
import uk.gov.justice.services.event.sourcing.subscription.manager.cdi.InterceptorContextProvider;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.Metadata;

import java.sql.Connection;
import java.sql.SQLException;
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
    public void shouldSendEventToEventInterceptorChain() throws Exception {

        final UUID eventId = randomUUID();
        final String name = "some-event-name";
        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String componentName = "some-component-name";
        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final Metadata metadata = mock(Metadata.class);
        final InterceptorContext interceptorContext = mock(InterceptorContext.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(eventJsonEnvelope.metadata()).thenReturn(metadata);

        when(metadata.id()).thenReturn(eventId);
        when(metadata.name()).thenReturn(name);
        when(metadata.streamId()).thenReturn(of(streamId));
        when(metadata.source()).thenReturn(of(source));
        when(interceptorContextProvider.getInterceptorContext(eventJsonEnvelope)).thenReturn(interceptorContext);

        subscriptionEventProcessor.processSingleEvent(eventJsonEnvelope, componentName);

        final InOrder inOrder = inOrder(
                streamStatusErrorPersistence,
                interceptorContextProvider,
                interceptorChainProcessor,
                streamProcessingFailureHandler,
                connection);

        inOrder.verify(streamStatusErrorPersistence).lockStreamForUpdate(streamId, source, componentName, connection);
        inOrder.verify(interceptorContextProvider).getInterceptorContext(eventJsonEnvelope);
        inOrder.verify(interceptorChainProcessor).process(interceptorContext);
        inOrder.verify(streamProcessingFailureHandler).onStreamProcessingSucceeded(eventJsonEnvelope, componentName);
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldRecordErrorIfEventProcessingFails() throws Exception {

        final UUID eventId = fromString("ba0c36e1-659e-430c-9d33-67eda5ca70cd");
        final String name = "some-event-name";
        final UUID streamId = fromString("4f4815fa-825d-4869-a37f-e443dea21d18");
        final String source = "some-source";
        final String componentName = "some-component-name";
        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);

        final NullPointerException nullPointerException = new NullPointerException("Oh my giddy aunt");

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final Metadata metadata = mock(Metadata.class);
        final InterceptorContext interceptorContext = mock(InterceptorContext.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(eventJsonEnvelope.metadata()).thenReturn(metadata);

        when(metadata.id()).thenReturn(eventId);
        when(metadata.name()).thenReturn(name);
        when(metadata.streamId()).thenReturn(of(streamId));
        when(metadata.source()).thenReturn(of(source));
        when(interceptorContextProvider.getInterceptorContext(eventJsonEnvelope)).thenReturn(interceptorContext);
        doThrow(nullPointerException).when(interceptorChainProcessor).process(interceptorContext);

        final StreamProcessingException streamProcessingException = assertThrows(
                StreamProcessingException.class,
                () -> subscriptionEventProcessor.processSingleEvent(eventJsonEnvelope, componentName));

        assertThat(streamProcessingException.getCause(), is(nullPointerException));
        assertThat(streamProcessingException.getMessage(), is("Failed to process event. name: 'some-event-name', eventId: 'ba0c36e1-659e-430c-9d33-67eda5ca70cd', streamId: '4f4815fa-825d-4869-a37f-e443dea21d18'"));

        final InOrder inOrder = inOrder(
                streamStatusErrorPersistence,
                interceptorContextProvider,
                interceptorChainProcessor,
                streamProcessingFailureHandler,
                connection);

        inOrder.verify(streamStatusErrorPersistence).lockStreamForUpdate(streamId, source, componentName, connection);
        inOrder.verify(interceptorContextProvider).getInterceptorContext(eventJsonEnvelope);
        inOrder.verify(interceptorChainProcessor).process(interceptorContext);
        inOrder.verify(streamProcessingFailureHandler).onStreamProcessingFailure(eventJsonEnvelope, nullPointerException, componentName);
        inOrder.verify(connection).close();

        verify(streamProcessingFailureHandler, never()).onStreamProcessingSucceeded(eventJsonEnvelope, componentName);
    }

    @Test
    public void shouldThrowStreamProcessingExceptionIfGettingDatabaseConnectionFails() throws Exception {

        final String componentName = "some-component-name";
        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);

        final SQLException sqlException = new SQLException("Lawks");

        final DataSource dataSource = mock(DataSource.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenThrow(sqlException);

        final StreamProcessingException streamProcessingException = assertThrows(
                StreamProcessingException.class,
                () -> subscriptionEventProcessor.processSingleEvent(eventJsonEnvelope, componentName));

        assertThat(streamProcessingException.getCause(), is(sqlException));
        assertThat(streamProcessingException.getMessage(), is("Failed to get database connection to viewstore"));
    }
}