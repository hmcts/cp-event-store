package uk.gov.justice.services.event.sourcing.subscription.manager;

import uk.gov.justice.services.event.sourcing.subscription.manager.cdi.EventSourceNameQualifier;
import uk.gov.justice.services.eventsourcing.source.api.service.core.LinkedEventSource;

import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

public class LinkedEventSourceProvider {

    @Inject
    @Any
    Instance<LinkedEventSource> publishedEventSources;

    public LinkedEventSource getLinkedEventSource(final String eventSourceName) {

        return publishedEventSources
                .select(new EventSourceNameQualifier(eventSourceName))
                .get();
    }
}
