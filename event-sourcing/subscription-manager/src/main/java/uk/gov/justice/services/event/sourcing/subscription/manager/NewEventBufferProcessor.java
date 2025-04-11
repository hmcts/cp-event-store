package uk.gov.justice.services.event.sourcing.subscription.manager;

import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.Optional;

import javax.inject.Inject;

public class NewEventBufferProcessor {

    @Inject
    private NewEventBufferManager newEventBufferManager;

    @Inject
    private SubscriptionEventProcessor subscriptionEventProcessor;

    public void process(final JsonEnvelope incomingJsonEnvelope, final String componentName) {
            while (true) {
                final Optional<JsonEnvelope> nextFromEventBuffer = newEventBufferManager.getNextFromEventBuffer(incomingJsonEnvelope, componentName);
                if (nextFromEventBuffer.isPresent()) {
                    subscriptionEventProcessor.processSingleEvent(nextFromEventBuffer.get(), componentName);
                } else {
                    break;
                }
            }
        }
}
