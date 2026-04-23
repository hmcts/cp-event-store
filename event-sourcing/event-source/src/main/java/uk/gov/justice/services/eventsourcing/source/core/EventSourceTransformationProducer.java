package uk.gov.justice.services.eventsourcing.source.core;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

@ApplicationScoped
public class EventSourceTransformationProducer {

    @Inject
    private EventStreamManager eventStreamManager;

    @Produces
    public EventSourceTransformation eventSourceTransformation() {

        return new DefaultEventSourceTransformation(eventStreamManager);
    }
}
