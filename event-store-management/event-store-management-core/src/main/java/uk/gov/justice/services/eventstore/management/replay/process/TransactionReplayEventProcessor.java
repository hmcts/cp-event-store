package uk.gov.justice.services.eventstore.management.replay.process;

import static javax.transaction.Transactional.TxType.REQUIRES_NEW;

import uk.gov.justice.services.event.sourcing.subscription.manager.cdi.factories.SubscriptionManagerSelector;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.metrics.micrometer.counters.MicrometerMetricsCounters;
import uk.gov.justice.services.subscription.SubscriptionManager;

import javax.inject.Inject;
import javax.transaction.Transactional;

public class TransactionReplayEventProcessor {

    @Inject
    private SubscriptionManagerSelector subscriptionManagerSelector;

    @Inject
    private MicrometerMetricsCounters micrometerMetricsCounters;


    @Transactional(REQUIRES_NEW)
    public void process(final String componentName, final JsonEnvelope eventEnvelope) {
        micrometerMetricsCounters.incrementEventsReceivedCount();
        final SubscriptionManager subscriptionManager = subscriptionManagerSelector.selectFor(componentName);
        subscriptionManager.process(eventEnvelope);
    }
}
