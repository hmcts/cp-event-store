package uk.gov.justice.services.event.sourcing.subscription.manager;

import static uk.gov.justice.services.event.sourcing.subscription.manager.EventOrderingStatus.EVENT_CORRECTLY_ORDERED;

import uk.gov.justice.services.messaging.JsonEnvelope;

import javax.inject.Inject;

@SuppressWarnings("java:S1192")
public class NewSubscriptionManagerDelegate {

    @Inject
    private EventBufferAwareSubscriptionEventProcessor eventBufferAwareSubscriptionEventProcessor;

    @Inject
    private StreamStatusService streamStatusService;

    public void process(final JsonEnvelope incomingJsonEnvelope, final String componentName) {

        final EventOrderingStatus eventOrderingStatus = streamStatusService.handleStreamStatusUpdates(
                incomingJsonEnvelope,
                componentName);

        if (eventOrderingStatus == EVENT_CORRECTLY_ORDERED) {
            eventBufferAwareSubscriptionEventProcessor.process(incomingJsonEnvelope, componentName);
        }
    }
}
