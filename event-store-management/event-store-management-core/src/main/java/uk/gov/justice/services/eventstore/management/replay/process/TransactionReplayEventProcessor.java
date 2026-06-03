package uk.gov.justice.services.eventstore.management.replay.process;

import static jakarta.transaction.Transactional.TxType.REQUIRES_NEW;

import uk.gov.justice.services.event.sourcing.subscription.manager.cdi.factories.SubscriptionManagerSelector;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.metrics.micrometer.counters.MicrometerMetricsCounters;
import uk.gov.justice.services.subscription.SubscriptionManager;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

@ApplicationScoped
public class TransactionReplayEventProcessor {

    @Inject
    private SubscriptionManagerSelector subscriptionManagerSelector;

    @Inject
    private MicrometerMetricsCounters micrometerMetricsCounters;


    @Transactional(REQUIRES_NEW)
    public void process(final String source, final String component, final JsonEnvelope eventEnvelope) {

        micrometerMetricsCounters.incrementEventsReceivedCount(source, component);
        final SubscriptionManager subscriptionManager = subscriptionManagerSelector.selectFor(component);
        subscriptionManager.process(eventEnvelope);
    }
}
