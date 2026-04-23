package uk.gov.justice.subscription.registry;

import uk.gov.justice.subscription.EventSourcesParser;
import uk.gov.justice.subscription.YamlFileFinder;
import uk.gov.justice.subscription.domain.eventsource.DefaultEventSourceDefinitionFactory;
import uk.gov.justice.subscription.domain.eventsource.EventSourceDefinition;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.stream.Stream;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

/**
 * Producer for the {@link EventSourceDefinitionRegistry} creates a single instance and returns the
 * same instance.
 */
@ApplicationScoped
public class EventSourceDefinitionRegistryProducer {

    @Inject
    EventSourcesParser eventSourcesParser;

    @Inject
    YamlFileFinder yamlFileFinder;

    @Inject
    DefaultEventSourceDefinitionFactory defaultEventSourceDefinitionFactory;

    private EventSourceDefinitionRegistry eventSourceDefinitionRegistry;

    /**
     * Either creates the single instance of the {@link EventSourceDefinitionRegistry} and returns
     * it, or returns the previously created instance.
     *
     * @return the instance of the {@link EventSourceDefinitionRegistry}
     */
    @Produces
    public EventSourceDefinitionRegistry getEventSourceDefinitionRegistry() {

        if (null == eventSourceDefinitionRegistry) {

            eventSourceDefinitionRegistry = new EventSourceDefinitionRegistry();

            try {
                final List<URL> eventSourcesPaths = yamlFileFinder.getEventSourcesPaths();

                if (eventSourcesPaths.isEmpty()) {
                    final EventSourceDefinition eventSourceDefinition = defaultEventSourceDefinitionFactory.createDefaultEventSource();
                    eventSourceDefinitionRegistry.register(eventSourceDefinition);
                } else {
                    final Stream<EventSourceDefinition> eventSourcesFrom = eventSourcesParser.eventSourcesFrom(eventSourcesPaths);
                    eventSourcesFrom.forEach(eventSourceDefinition -> eventSourceDefinitionRegistry.register(eventSourceDefinition));
                }
            } catch (final IOException e) {
                throw new RegistryException("Failed to find yaml/event-sources.yaml resources on the classpath", e);
            }
        }
        
        return eventSourceDefinitionRegistry;
    }
}
