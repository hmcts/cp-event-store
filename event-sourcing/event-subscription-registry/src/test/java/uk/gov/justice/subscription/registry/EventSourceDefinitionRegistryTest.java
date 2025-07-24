package uk.gov.justice.subscription.registry;

import static java.util.Optional.empty;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static uk.gov.justice.subscription.domain.builders.EventSourceDefinitionBuilder.eventSourceDefinition;

import uk.gov.justice.subscription.domain.eventsource.EventSourceDefinition;
import uk.gov.justice.subscription.domain.eventsource.Location;

import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EventSourceDefinitionRegistryTest {

    @Test
    public void shouldReturnEventSourceDefinition() {

        final EventSourceDefinition eventSourceDefinition1 = eventSourceDefinition()
                .withLocation(mock(Location.class))
                .withName("eventSourceDefinition1")
                .build();

        final EventSourceDefinition eventSourceDefinition2 = eventSourceDefinition()
                .withLocation(mock(Location.class))
                .withName("eventSourceDefinition2").build();


        final EventSourceDefinitionRegistry eventSourceDefinitionRegistry = new EventSourceDefinitionRegistry();

        eventSourceDefinitionRegistry.register(eventSourceDefinition1);
        eventSourceDefinitionRegistry.register(eventSourceDefinition2);

        final Optional<EventSourceDefinition> defaultEventSourceDefinition = eventSourceDefinitionRegistry.getEventSourceDefinitionFor("eventSourceDefinition1");

        assertThat(defaultEventSourceDefinition, is(Optional.of(eventSourceDefinition1)));
    }

    @Test
    public void shouldReturnDefaultEventSourceDefinition() {

        final Location location = new Location("", null, "dataSource");

        final EventSourceDefinition eventSourceDefinition1 = eventSourceDefinition()
                .withLocation(location)
                .withName("eventSourceDefinition1")
                .withDefault(true)
                .build();

        final EventSourceDefinition eventSourceDefinition2 = eventSourceDefinition()
                .withLocation(mock(Location.class))
                .withName("eventSourceDefinition2")
                .build();

        final EventSourceDefinitionRegistry eventSourceDefinitionRegistry = new EventSourceDefinitionRegistry();

        eventSourceDefinitionRegistry.register(eventSourceDefinition1);
        eventSourceDefinitionRegistry.register(eventSourceDefinition2);


        final EventSourceDefinition defaultEventSourceDefinition = eventSourceDefinitionRegistry.getDefaultEventSourceDefinition();

        assertThat(defaultEventSourceDefinition.getName(), is("eventSourceDefinition1"));
        assertTrue(defaultEventSourceDefinition.isDefault());
    }

    @Test
    public void shouldThrowExceptionIfSecondDefaultEventSourceIsAdded() {

        final Location location1 = new Location("", null, "dataSource");
        final Location location2 = new Location("", null, "dataSource");

        final EventSourceDefinition eventSourceDefinition1 = eventSourceDefinition()
                .withLocation(location1)
                .withName("eventSourceDefinition1")
                .withDefault(true)
                .build();

        final EventSourceDefinition eventSourceDefinition2 = eventSourceDefinition()
                .withLocation(location2)
                .withDefault(true)
                .withName("eventSourceDefinition2").build();


        final EventSourceDefinitionRegistry eventSourceDefinitionRegistry = new EventSourceDefinitionRegistry();

        try {
            eventSourceDefinitionRegistry.register(eventSourceDefinition1);
            eventSourceDefinitionRegistry.register(eventSourceDefinition2);
            fail();
        } catch (final RegistryException expected) {
            assertThat(expected.getMessage(), is("You cannot define more than one default event source"));
        }
    }

    @Test
    public void shouldThrowExceptionIfNoDataSourceDefinedForDefaultEventSource() {

        final Location location1 = new Location("", null, null);
        final Location location2 = new Location("", null, null);

        final EventSourceDefinition eventSourceDefinition1 = eventSourceDefinition()
                .withLocation(location1)
                .withName("eventSourceDefinition1")
                .withDefault(true)
                .build();

        final EventSourceDefinition eventSourceDefinition2 = eventSourceDefinition()
                .withLocation(location2)
                .withDefault(false)
                .withName("eventSourceDefinition2").build();

        final EventSourceDefinitionRegistry eventSourceDefinitionRegistry = new EventSourceDefinitionRegistry();

        try {
            eventSourceDefinitionRegistry.register(eventSourceDefinition1);
            eventSourceDefinitionRegistry.register(eventSourceDefinition2);
            fail();
        } catch (final RegistryException expected) {
            assertThat(expected.getMessage(), is("You must define data_source for default event source"));
        }
    }

    @Test
    public void shouldThrowExceptionIfNoDefaultDataSourceDefined() {

        final EventSourceDefinition eventSourceDefinition1 = eventSourceDefinition()
                .withName("eventSourceDefinition1")
                .build();

        final EventSourceDefinition eventSourceDefinition2 = eventSourceDefinition()
                .withName("eventSourceDefinition2").build();

        final EventSourceDefinitionRegistry eventSourceDefinitionRegistry = new EventSourceDefinitionRegistry();

        try {
            eventSourceDefinitionRegistry.register(eventSourceDefinition1);
            eventSourceDefinitionRegistry.register(eventSourceDefinition2);
            //Test
            eventSourceDefinitionRegistry.getDefaultEventSourceDefinition();
            fail();
        } catch (final RegistryException expected) {
            assertThat(expected.getMessage(), is("You must define a default event source"));
        }
    }


    @Test
    public void shouldReturnEmptyEventSourceDefinitionIfEventSourceNameNotFound() {

        final EventSourceDefinition eventSourceDefinition1 = eventSourceDefinition()
                .withName("eventSourceDefinition1")
                .build();

        final EventSourceDefinitionRegistry eventSourceDefinitionRegistry = new EventSourceDefinitionRegistry();
        eventSourceDefinitionRegistry.register(eventSourceDefinition1);

        assertThat(eventSourceDefinitionRegistry.getEventSourceDefinitionFor("nonExitingEventSourceDefinition"), is(empty()));
    }


}
