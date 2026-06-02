package uk.gov.justice.services.event.sourcing.subscription.manager;

import static jakarta.transaction.Transactional.TxType.REQUIRED;

import uk.gov.justice.services.core.interceptor.InterceptorChainProcessor;
import uk.gov.justice.services.event.sourcing.subscription.manager.cdi.InterceptorContextProvider;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.subscription.SubscriptionManager;

import jakarta.transaction.Transactional;

public class BackwardsCompatibleSubscriptionManager implements SubscriptionManager {

    private final InterceptorChainProcessor interceptorChainProcessor;
    private final InterceptorContextProvider interceptorContextProvider;

    public BackwardsCompatibleSubscriptionManager(
            final InterceptorChainProcessor interceptorChainProcessor,
            final InterceptorContextProvider interceptorContextProvider) {
        this.interceptorChainProcessor = interceptorChainProcessor;
        this.interceptorContextProvider = interceptorContextProvider;
    }

    @Transactional(REQUIRED)
    @Override
    public void process(final JsonEnvelope incomingJsonEnvelope) {
        interceptorChainProcessor.process(
                interceptorContextProvider.getInterceptorContext(incomingJsonEnvelope)
        );
    }
}
