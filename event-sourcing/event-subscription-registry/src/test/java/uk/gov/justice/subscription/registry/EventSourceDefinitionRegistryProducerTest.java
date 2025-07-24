package uk.gov.justice.subscription.registry;

import static java.util.Arrays.asList;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.justice.subscription.domain.builders.EventSourceDefinitionBuilder.eventSourceDefinition;

import uk.gov.justice.subscription.EventSourcesParser;
import uk.gov.justice.subscription.YamlFileFinder;
import uk.gov.justice.subscription.domain.eventsource.DefaultEventSourceDefinitionFactory;
import uk.gov.justice.subscription.domain.eventsource.EventSourceDefinition;
import uk.gov.justice.subscription.domain.eventsource.Location;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EventSourceDefinitionRegistryProducerTest {

    @Mock
    private YamlFileFinder yamlFileFinder;

    @Mock
    private EventSourcesParser eventSourcesParser;

    @Mock
    private DefaultEventSourceDefinitionFactory defaultEventSourceDefinitionFactory;

    @InjectMocks
    private EventSourceDefinitionRegistryProducer eventSourceDefinitionRegistryProducer;

    @Test
    public void shouldCreateARegistryOfAllEventSourceDefinitionsFromTheClasspath() throws Exception {

        final String event_source_name_1 = "event_source_name_1";
        final String event_source_name_2 = "event_source_name_2";

        final URL url_1 = new URL("file:/test");
        final URL url_2 = new URL("file:/test");

        final EventSourceDefinition eventSourceDefinition1 = eventSourceDefinition()
                .withLocation(mock(Location.class))
                .withName(event_source_name_1).build();

        final EventSourceDefinition eventSourceDefinition2 = eventSourceDefinition()
                .withLocation(mock(Location.class))
                .withName(event_source_name_2).build();

        final List<URL> pathList = asList(url_1, url_2);

        when(yamlFileFinder.getEventSourcesPaths()).thenReturn(pathList);
        when(eventSourcesParser.eventSourcesFrom(pathList)).thenReturn(Stream.of(eventSourceDefinition1, eventSourceDefinition2));

        final EventSourceDefinitionRegistry eventSourceDefinitionRegistry = eventSourceDefinitionRegistryProducer.getEventSourceDefinitionRegistry();

        assertThat(eventSourceDefinitionRegistry, is(notNullValue()));

        assertThat(eventSourceDefinitionRegistry.getEventSourceDefinitionFor(event_source_name_1), is(of(eventSourceDefinition1)));
        assertThat(eventSourceDefinitionRegistry.getEventSourceDefinitionFor(event_source_name_2), is(of(eventSourceDefinition2)));
    }

    @Test
    public void shouldCreateSingleRegistryAndReturnSameInstance() throws Exception {

        final String event_source_name_1 = "event_source_name_1";
        final String event_source_name_2 = "event_source_name_2";

        final URL url_1 = new URL("file:/test");
        final URL url_2 = new URL("file:/test");

        final Location location1 = new Location("", null, "dataSource");
        final Location location2 = new Location("", null, null);

        final EventSourceDefinition eventSourceDefinition1 = eventSourceDefinition()
                .withLocation(location1)
                .withDefault(true)
                .withName(event_source_name_1).build();

        final EventSourceDefinition eventSourceDefinition2 = eventSourceDefinition()
                .withLocation(location2)
                .withName(event_source_name_2).build();

        final List<URL> pathList = asList(url_1, url_2);

        when(yamlFileFinder.getEventSourcesPaths()).thenReturn(pathList);
        when(eventSourcesParser.eventSourcesFrom(pathList)).thenReturn(Stream.of(eventSourceDefinition1, eventSourceDefinition2));

        final EventSourceDefinitionRegistry eventSourceDefinitionRegistry_1 = eventSourceDefinitionRegistryProducer.getEventSourceDefinitionRegistry();
        final EventSourceDefinitionRegistry eventSourceDefinitionRegistry_2 = eventSourceDefinitionRegistryProducer.getEventSourceDefinitionRegistry();

        assertThat(eventSourceDefinitionRegistry_1, is(sameInstance(eventSourceDefinitionRegistry_2)));
    }

    @Test
    public void shouldCreateADefaultEventSourceDefinitionIfNoEventSourceUrlsAreFound() throws Exception {

        when(yamlFileFinder.getEventSourcesPaths()).thenReturn(new ArrayList<>());

        final EventSourceDefinition defaultEventSourceDefinition = new EventSourceDefinition(
                "name",
                true,
                new Location("jms uri", "rest uri", "data source name")
        );

        when(defaultEventSourceDefinitionFactory.createDefaultEventSource()).thenReturn(defaultEventSourceDefinition);

        final EventSourceDefinitionRegistry eventSourceDefinitionRegistry =
                eventSourceDefinitionRegistryProducer.getEventSourceDefinitionRegistry();

        assertThat(eventSourceDefinitionRegistry.getDefaultEventSourceDefinition(), is(defaultEventSourceDefinition));
    }

    @Test
    public void shouldThrowExceptionIfIOExceptionOccursWhenFindingEventSourcesOnTheClasspath() throws Exception {

        when(yamlFileFinder.getEventSourcesPaths()).thenThrow(new IOException());

        try {
            eventSourceDefinitionRegistryProducer.getEventSourceDefinitionRegistry();
            fail();
        } catch (final RegistryException e) {
            assertThat(e.getMessage(), is("Failed to find yaml/event-sources.yaml resources on the classpath"));
            assertThat(e.getCause(), is(instanceOf(IOException.class)));
        }
    }
}
