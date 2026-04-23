package uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.manager.cdi;

import uk.gov.justice.services.common.configuration.errors.event.EventErrorHandlingConfiguration;
import uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.manager.CatchupEventProcessor;
import uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.manager.DefaultTransactionalEventProcessor;
import uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.manager.NewSubscriptionAwareEventProcessor;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

@ApplicationScoped
public class CatchupEventProcessorProducer {

    @Inject
    private EventErrorHandlingConfiguration eventErrorHandlingConfiguration;

    @Inject
    private NewSubscriptionAwareEventProcessor newSubscriptionAwareEventProcessor;

    @Inject
    private DefaultTransactionalEventProcessor defaultTransactionalEventProcessor;

    @Produces
    public CatchupEventProcessor transactionalEventProcessor() {
        if (eventErrorHandlingConfiguration.isEventStreamSelfHealingEnabled()) {
            return newSubscriptionAwareEventProcessor::processWithEventBuffer;
        } else {
            return defaultTransactionalEventProcessor::processWithEventBuffer;
        }
    }
}



