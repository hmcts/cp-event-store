package uk.gov.justice.services.eventstore.management.replay.process;

import javax.inject.Inject;
import javax.transaction.Transactional;
import uk.gov.justice.services.event.sourcing.subscription.manager.cdi.factories.SubscriptionManagerSelector;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.subscription.SubscriptionManager;

import static javax.transaction.Transactional.TxType.REQUIRES_NEW;

public class TransactionReplayEventProcessor {

    @Inject
    private SubscriptionManagerSelector subscriptionManagerSelector;


    @Transactional(REQUIRES_NEW)
    public void process(final String componentName, final JsonEnvelope eventEnvelope) {
        final SubscriptionManager subscriptionManager = subscriptionManagerSelector.selectFor(componentName);
        subscriptionManager.process(eventEnvelope);
    }
}
