package uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.manager;

import java.util.concurrent.ConcurrentHashMap;

import jakarta.inject.Inject;

public class EventsInProcessCounterProvider {

    @Inject
    private EventQueueProcessingConfig eventQueueProcessingConfig;

    private ConcurrentHashMap<String, EventsInProcessCounter> concurrentHashMap = new ConcurrentHashMap<>();

    public EventsInProcessCounter getInstance() {
        return concurrentHashMap.computeIfAbsent("default", this::newInstance);
    }

    private EventsInProcessCounter newInstance(String s) {
        return new EventsInProcessCounter(eventQueueProcessingConfig.getMaxTotalEventsInProcess());
    }
}
