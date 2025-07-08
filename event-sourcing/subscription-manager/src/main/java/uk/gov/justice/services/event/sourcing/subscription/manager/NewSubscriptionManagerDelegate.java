package uk.gov.justice.services.event.sourcing.subscription.manager;

import static uk.gov.justice.services.event.sourcing.subscription.manager.EventOrderingStatus.EVENT_CORRECTLY_ORDERED;

import uk.gov.justice.services.eventsourcing.util.messaging.EventSourceNameCalculator;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.metrics.micrometer.counters.MicrometerMetricsCounters;

import javax.inject.Inject;

@SuppressWarnings("java:S1192")
public class NewSubscriptionManagerDelegate {

    @Inject
    private EventBufferAwareSubscriptionEventProcessor eventBufferAwareSubscriptionEventProcessor;

    @Inject
    private StreamStatusService streamStatusService;

    @Inject
    private MicrometerMetricsCounters micrometerMetricsCounters;

    @Inject
    private EventSourceNameCalculator eventSourceNameCalculator;

    public void process(final JsonEnvelope incomingJsonEnvelope, final String componentName) {

        final String source = eventSourceNameCalculator.getSource(incomingJsonEnvelope);
        micrometerMetricsCounters.incrementEventsReceivedCount(source, componentName);

        final EventOrderingStatus eventOrderingStatus = streamStatusService.handleStreamStatusUpdates(
                incomingJsonEnvelope,
                componentName);

        if (eventOrderingStatus == EVENT_CORRECTLY_ORDERED) {
            eventBufferAwareSubscriptionEventProcessor.process(incomingJsonEnvelope, componentName);
        }
    }
}
