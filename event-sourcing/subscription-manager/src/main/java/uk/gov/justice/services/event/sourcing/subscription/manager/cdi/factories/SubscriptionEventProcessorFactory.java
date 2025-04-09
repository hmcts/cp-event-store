package uk.gov.justice.services.event.sourcing.subscription.manager.cdi.factories;

import uk.gov.justice.services.core.interceptor.InterceptorChainProcessor;
import uk.gov.justice.services.core.interceptor.InterceptorChainProcessorProducer;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamStatusErrorPersistence;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamProcessingFailureHandler;
import uk.gov.justice.services.event.sourcing.subscription.error.SubscriptionEventProcessor;
import uk.gov.justice.services.event.sourcing.subscription.manager.cdi.InterceptorContextProvider;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;

import javax.inject.Inject;

public class SubscriptionEventProcessorFactory {

    @Inject
    private InterceptorContextProvider interceptorContextProvider;

    @Inject
    private StreamProcessingFailureHandler streamProcessingFailureHandler;

    @Inject
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    @Inject
    private StreamStatusErrorPersistence streamStatusErrorPersistence;

    @Inject
    private InterceptorChainProcessorProducer interceptorChainProcessorProducer;

    public SubscriptionEventProcessor create(final String componentName) {

        final InterceptorChainProcessor interceptorChainProcessor = interceptorChainProcessorProducer
                .produceLocalProcessor(componentName);

        return new SubscriptionEventProcessor(
                interceptorContextProvider,
                interceptorChainProcessor,
                viewStoreJdbcDataSourceProvider,
                streamProcessingFailureHandler,
                streamStatusErrorPersistence
        );
    }
}
