package uk.gov.justice.services.eventsourcing.source.core;

import uk.gov.justice.subscription.registry.EventSourceDefinitionRegistry;

import jakarta.inject.Inject;

public class EventSourceNameProvider {

    @Inject
    private EventSourceDefinitionRegistry eventSourceDefinitionRegistry;

    public String getDefaultEventSourceName() {
        return eventSourceDefinitionRegistry.getDefaultEventSourceDefinition().getName();
    }
}
