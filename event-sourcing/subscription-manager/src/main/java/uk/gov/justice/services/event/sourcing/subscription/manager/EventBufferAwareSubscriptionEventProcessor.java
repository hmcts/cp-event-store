package uk.gov.justice.services.event.sourcing.subscription.manager;

import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.Optional;

import jakarta.inject.Inject;

public class EventBufferAwareSubscriptionEventProcessor {

    @Inject
    private NewEventBufferManager newEventBufferManager;

    @Inject
    private SubscriptionEventProcessor subscriptionEventProcessor;

    public void process(final JsonEnvelope incomingJsonEnvelope, final String componentName) {

        if(subscriptionEventProcessor.processSingleEvent(incomingJsonEnvelope, componentName)) {
            JsonEnvelope previouslyProcessedEvent = incomingJsonEnvelope;
            while (true) {
                final Optional<JsonEnvelope> nextFromEventBuffer = newEventBufferManager.getNextFromEventBuffer(previouslyProcessedEvent, componentName);
                if (nextFromEventBuffer.isPresent()) {
                    final JsonEnvelope nextJsonEnvelopeFromBuffer = nextFromEventBuffer.get();
                    subscriptionEventProcessor.processSingleEvent(nextJsonEnvelopeFromBuffer, componentName);
                    previouslyProcessedEvent = nextJsonEnvelopeFromBuffer;
                } else {
                    break;
                }
            }
        }
    }
}
