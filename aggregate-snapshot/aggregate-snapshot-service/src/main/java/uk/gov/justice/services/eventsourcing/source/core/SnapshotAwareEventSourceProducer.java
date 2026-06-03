package uk.gov.justice.services.eventsourcing.source.core;

import static java.lang.String.format;

import uk.gov.justice.subscription.domain.eventsource.EventSourceDefinition;
import uk.gov.justice.subscription.domain.eventsource.Location;
import uk.gov.justice.subscription.registry.EventSourceDefinitionRegistry;

import java.util.Optional;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.CreationException;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

@ApplicationScoped
@Alternative
@Priority(100)
public class SnapshotAwareEventSourceProducer {

    @Inject
    private EventSourceDefinitionRegistry eventSourceDefinitionRegistry;

    @Inject
    private SnapshotAwareEventSourceFactory snapshotAwareEventSourceFactory;

    /**
     * Produces Default EventSource injection point.
     *
     * @return {@link EventSource}
     */
    @Produces
    public EventSource eventSource() {

        final EventSourceDefinition eventSourceDefinition = eventSourceDefinitionRegistry.getDefaultEventSourceDefinition();
        final Location location = eventSourceDefinition.getLocation();
        final Optional<String> dataSourceOptional = location.getDataSource();

        return dataSourceOptional
                .map(dataSource -> snapshotAwareEventSourceFactory.create(eventSourceDefinition.getName()))
                .orElseThrow(() -> new CreationException(
                        format("No DataSource specified for EventSource '%s' specified in event-sources.yaml", eventSourceDefinition.getName())
                ));
    }

}
