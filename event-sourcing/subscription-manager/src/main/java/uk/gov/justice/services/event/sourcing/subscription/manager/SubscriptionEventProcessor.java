package uk.gov.justice.services.event.sourcing.subscription.manager;

import static java.lang.String.format;
import static javax.transaction.Transactional.TxType.REQUIRES_NEW;

import uk.gov.justice.services.core.interceptor.InterceptorChainProcessor;
import uk.gov.justice.services.core.interceptor.InterceptorContext;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamStatusErrorPersistence;
import uk.gov.justice.services.event.sourcing.subscription.error.MissingSourceException;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamProcessingException;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamProcessingFailureHandler;
import uk.gov.justice.services.event.sourcing.subscription.manager.cdi.InterceptorContextProvider;
import uk.gov.justice.services.eventsourcing.source.api.streams.MissingStreamIdException;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.Metadata;

import java.util.UUID;

import javax.transaction.Transactional;

public class SubscriptionEventProcessor {

    private final InterceptorContextProvider interceptorContextProvider;
    private final InterceptorChainProcessor interceptorChainProcessor;
    private final StreamProcessingFailureHandler streamProcessingFailureHandler;

    public SubscriptionEventProcessor(
            final InterceptorContextProvider interceptorContextProvider,
            final InterceptorChainProcessor interceptorChainProcessor,
            final StreamProcessingFailureHandler streamProcessingFailureHandler) {
        this.interceptorContextProvider = interceptorContextProvider;
        this.interceptorChainProcessor = interceptorChainProcessor;
        this.streamProcessingFailureHandler = streamProcessingFailureHandler;
    }

    @Transactional(value = REQUIRES_NEW)
    public void processSingleEvent(final JsonEnvelope eventJsonEnvelope, final String componentName) {

        try {
            final InterceptorContext interceptorContext = interceptorContextProvider.getInterceptorContext(eventJsonEnvelope);
            interceptorChainProcessor.process(interceptorContext);
        } catch (final Throwable e) {
            streamProcessingFailureHandler.onStreamProcessingFailure(eventJsonEnvelope, e, componentName);
            throw new StreamProcessingException(
                    format("Failed to process event. name: '%s', eventId: '%s', streamId: '%s'",
                            eventJsonEnvelope.metadata().name(),
                            eventJsonEnvelope.metadata().id(),
                            eventJsonEnvelope.metadata().streamId().orElse(null)),
                    e);
        }

        streamProcessingFailureHandler.onStreamProcessingSucceeded(eventJsonEnvelope, componentName);
    }
}