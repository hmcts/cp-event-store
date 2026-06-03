package uk.gov.justice.services.eventsourcing.source.core;

import static java.lang.String.format;

import uk.gov.justice.subscription.domain.eventsource.EventSourceDefinition;
import uk.gov.justice.subscription.domain.eventsource.Location;
import uk.gov.justice.subscription.registry.EventSourceDefinitionRegistry;

import java.util.Optional;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.CreationException;
import jakarta.enterprise.inject.Default;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

/**
 * Producer for Default EventSource
 */
@ApplicationScoped
@Default
@Priority(200)
public class EventSourceProducer {

    @Inject
    private EventSourceDefinitionRegistry eventSourceDefinitionRegistry;

    @Inject
    private JdbcEventSourceFactory jdbcEventSourceFactory;

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
                .map(dataSource -> jdbcEventSourceFactory.create(eventSourceDefinition.getName()))
                .orElseThrow(() -> new CreationException(
                        format("No DataSource specified for EventSource '%s' specified in event-sources.yaml", eventSourceDefinition.getName())
                ));
    }

}
