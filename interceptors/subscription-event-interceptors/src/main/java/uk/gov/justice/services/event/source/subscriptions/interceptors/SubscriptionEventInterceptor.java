package uk.gov.justice.services.event.source.subscriptions.interceptors;

import uk.gov.justice.services.common.configuration.subscription.pull.EventPullConfiguration;
import uk.gov.justice.services.core.interceptor.Interceptor;
import uk.gov.justice.services.core.interceptor.InterceptorChain;
import uk.gov.justice.services.core.interceptor.InterceptorContext;
import uk.gov.justice.services.subscription.ProcessedEventTrackingService;

import javax.inject.Inject;

public class SubscriptionEventInterceptor implements Interceptor {

    @Inject
    private ProcessedEventTrackingService processedEventTrackingService;

    @Inject
    private EventPullConfiguration eventPullConfiguration;

    @Override
    public InterceptorContext process(final InterceptorContext interceptorContext, final InterceptorChain interceptorChain) {

        final InterceptorContext resultInterceptorContext = interceptorChain.processNext(interceptorContext);

        if (! eventPullConfiguration.shouldProcessEventsByPullMechanism()) {
            processedEventTrackingService.trackProcessedEvent(
                    resultInterceptorContext.inputEnvelope(),
                    resultInterceptorContext.getComponentName());
        }

        return resultInterceptorContext;
    }
}
