package uk.gov.justice.services.event.sourcing.subscription.error;

import static java.lang.String.format;
import static javax.transaction.Transactional.TxType.REQUIRES_NEW;

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

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.transaction.Transactional;

import com.fasterxml.jackson.databind.ext.SqlBlobSerializer;

public class SubscriptionEventProcessor {

    private final InterceptorContextProvider interceptorContextProvider;
    private final InterceptorChainProcessor interceptorChainProcessor;
    private final ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;
    private final StreamProcessingFailureHandler streamProcessingFailureHandler;
    private final StreamStatusErrorPersistence streamStatusErrorPersistence;

    public SubscriptionEventProcessor(
            final InterceptorContextProvider interceptorContextProvider,
            final InterceptorChainProcessor interceptorChainProcessor,
            final ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider,
            final StreamProcessingFailureHandler streamProcessingFailureHandler,
            final StreamStatusErrorPersistence streamStatusErrorPersistence) {
        this.interceptorContextProvider = interceptorContextProvider;
        this.interceptorChainProcessor = interceptorChainProcessor;
        this.viewStoreJdbcDataSourceProvider = viewStoreJdbcDataSourceProvider;
        this.streamProcessingFailureHandler = streamProcessingFailureHandler;
        this.streamStatusErrorPersistence = streamStatusErrorPersistence;
    }

    @Transactional(value = REQUIRES_NEW, dontRollbackOn = Exception.class)
    public void processSingleEvent(final JsonEnvelope eventJsonEnvelope, final String componentName) {

        try (final Connection connection = viewStoreJdbcDataSourceProvider.getDataSource().getConnection()) {
            final Metadata metadata = eventJsonEnvelope.metadata();
            final UUID eventId = metadata.id();
            final String name = metadata.name();
            final UUID streamId = metadata.streamId().orElseThrow(() -> new MissingStreamIdException(format("No streamId found in event: name '%s', eventId '%s'", name, eventId)));
            final String source = metadata.source().orElseThrow(() -> new MissingSourceException(format("No source found in event: name '%s', eventId '%s'", name, eventId)));

            streamStatusErrorPersistence.lockStreamForUpdate(streamId, source, componentName, connection);
            final Savepoint savepoint = connection.setSavepoint();
            try {
                final InterceptorContext interceptorContext = interceptorContextProvider.getInterceptorContext(eventJsonEnvelope);
                interceptorChainProcessor.process(interceptorContext);
            } catch (final Throwable e) {
                connection.rollback(savepoint);
                streamProcessingFailureHandler.onStreamProcessingFailure(eventJsonEnvelope, e, componentName);
                throw new StreamProcessingException(
                        format("Failed to process event. name: '%s', eventId: '%s', streamId: '%s'",
                                name,
                                eventId,
                                streamId),
                        e);
            }

            streamProcessingFailureHandler.onStreamProcessingSucceeded(eventJsonEnvelope, componentName);
            connection.releaseSavepoint(savepoint);

        } catch (final SQLException e) {
            throw new StreamErrorHandlingException("Failed to get database connection to viewstore", e);
        }
    }
}
