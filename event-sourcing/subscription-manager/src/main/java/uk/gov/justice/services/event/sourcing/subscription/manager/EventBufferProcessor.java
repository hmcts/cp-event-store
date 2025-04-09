package uk.gov.justice.services.event.sourcing.subscription.manager;

import uk.gov.justice.services.event.buffer.api.EventBufferService;
import uk.gov.justice.services.event.sourcing.subscription.error.SubscriptionEventProcessor;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.stream.Stream;

public class EventBufferProcessor {

    private final EventBufferService eventBufferService;
    private final SubscriptionEventProcessor subscriptionEventProcessor;
    private final String component;

    public EventBufferProcessor(
            final EventBufferService eventBufferService,
            final SubscriptionEventProcessor subscriptionEventProcessor,
            final String component) {
        this.eventBufferService = eventBufferService;
        this.subscriptionEventProcessor = subscriptionEventProcessor;
        this.component = component;
    }

    public void processWithEventBuffer(final JsonEnvelope incomingJsonEnvelope) {

        try (final Stream<JsonEnvelope> jsonEnvelopeStream = eventBufferService.currentOrderedEventsWith(incomingJsonEnvelope, component)) {
            jsonEnvelopeStream.forEach(eventJsonEnvelope -> {
                subscriptionEventProcessor.processSingleEvent(eventJsonEnvelope, component);
            });
        }
    }
}

