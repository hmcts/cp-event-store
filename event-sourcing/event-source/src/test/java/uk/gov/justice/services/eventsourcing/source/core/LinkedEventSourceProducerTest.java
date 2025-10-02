package uk.gov.justice.services.eventsourcing.source.core;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static uk.gov.justice.subscription.domain.builders.EventSourceDefinitionBuilder.eventSourceDefinition;

import uk.gov.justice.services.cdi.QualifierAnnotationExtractor;
import uk.gov.justice.services.eventsourcing.source.core.annotation.EventSourceName;
import uk.gov.justice.subscription.domain.eventsource.EventSourceDefinition;
import uk.gov.justice.subscription.domain.eventsource.Location;
import uk.gov.justice.subscription.registry.EventSourceDefinitionRegistry;

import java.util.Optional;

import javax.enterprise.inject.CreationException;
import javax.enterprise.inject.spi.InjectionPoint;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class LinkedEventSourceProducerTest {

    private static final String EMPTY_STRING = "";
    @Mock
    private EventSourceDefinitionRegistry eventSourceDefinitionRegistry;

    @Mock
    private JdbcLinkedEventSourceFactory jdbcLinkedEventSourceFactory;

    @Mock
    private QualifierAnnotationExtractor qualifierAnnotationExtractor;

    @InjectMocks
    private LinkedEventSourceProducer linkedEventSourceProducer;

    @Test
    public void shouldCreateDefaultEventSourceDefinitionWhenEventSourceNameIsEmpty() throws Exception {

        final EventSourceDefinition eventSourceDefinition = eventSourceDefinition()
                .withName("defaultEventSource")
                .withDefault(true)
                .withLocation(new Location("", empty(), Optional.of("dataSource")))
                .build();
        final InjectionPoint injectionPoint = mock(InjectionPoint.class);
        final EventSourceName eventSourceNameAnnotation = mock(EventSourceName.class);
        final DefaultLinkedEventSource defaultPublishedEventSource = mock(DefaultLinkedEventSource.class);

        when(eventSourceDefinitionRegistry.getDefaultEventSourceDefinition()).thenReturn(eventSourceDefinition);
        when(jdbcLinkedEventSourceFactory.create(eventSourceDefinition.getLocation().getDataSource().get())).thenReturn(defaultPublishedEventSource);

        assertThat(linkedEventSourceProducer.linkedEventSource(), is(defaultPublishedEventSource));
    }

    @Test
    public void shouldCreateDefaultEventSourceWhenNoEventSourceNameQualifierSet() throws Exception {

        final EventSourceDefinition eventSourceDefinition = eventSourceDefinition()
                .withName("defaultEventSource")
                .withDefault(true)
                .withLocation(new Location("", empty(), Optional.of("dataSource")))
                .build();

        final DefaultLinkedEventSource defaultPublishedEventSource = mock(DefaultLinkedEventSource.class);

        when(eventSourceDefinitionRegistry.getDefaultEventSourceDefinition()).thenReturn(eventSourceDefinition);
        when(jdbcLinkedEventSourceFactory.create(eventSourceDefinition.getLocation().getDataSource().get())).thenReturn(defaultPublishedEventSource);

        assertThat(linkedEventSourceProducer.linkedEventSource(), is(defaultPublishedEventSource));
    }

    @Test
    public void shouldCreateAnEventSourceUsingTheEventSourceNameAnnotation() throws Exception {

        final String eventSourceName = "eventSourceName";
        final String dataSource = "my-data-source";

        final InjectionPoint injectionPoint = mock(InjectionPoint.class);
        final EventSourceName eventSourceNameAnnotation = mock(EventSourceName.class);
        final EventSourceDefinition eventSourceDefinition = mock(EventSourceDefinition.class);
        final Location location = mock(Location.class);
        final DefaultLinkedEventSource defaultPublishedEventSource = mock(DefaultLinkedEventSource.class);

        when(qualifierAnnotationExtractor.getFrom(injectionPoint, EventSourceName.class)).thenReturn(eventSourceNameAnnotation);
        when(eventSourceNameAnnotation.value()).thenReturn(eventSourceName);
        when(eventSourceDefinitionRegistry.getEventSourceDefinitionFor(eventSourceName)).thenReturn(Optional.of(eventSourceDefinition));
        when(eventSourceDefinition.getLocation()).thenReturn(location);
        when(location.getDataSource()).thenReturn(of(dataSource));
        when(jdbcLinkedEventSourceFactory.create(dataSource)).thenReturn(defaultPublishedEventSource);

        assertThat(linkedEventSourceProducer.linkedEventSource(injectionPoint), is(defaultPublishedEventSource));
    }

    @Test
    public void shouldFailIfNoEventSourceFoundInTheEventSourceRegistry() throws Exception {

        final String eventSourceName = "my-event-source";

        final InjectionPoint injectionPoint = mock(InjectionPoint.class);
        final EventSourceName eventSourceNameAnnotation = mock(EventSourceName.class);

        when(qualifierAnnotationExtractor.getFrom(injectionPoint, EventSourceName.class)).thenReturn(eventSourceNameAnnotation);
        when(eventSourceNameAnnotation.value()).thenReturn(eventSourceName);
        when(eventSourceDefinitionRegistry.getEventSourceDefinitionFor(eventSourceName)).thenReturn(empty());

        try {
            linkedEventSourceProducer.linkedEventSource(injectionPoint);
            fail();
        } catch (final CreationException expected) {
            assertThat(expected.getMessage(), is("Failed to find EventSource named 'my-event-source' in event-sources.yaml"));
        }

        verifyNoInteractions(jdbcLinkedEventSourceFactory);
    }

    @Test
    public void shouldFailIfNoDataSourceNameFoundInEventSourcesYaml() throws Exception {

        final String eventSourceName = "eventSourceName";
        final String dataSourceName = "my-data-source";

        final InjectionPoint injectionPoint = mock(InjectionPoint.class);
        final EventSourceName eventSourceNameAnnotation = mock(EventSourceName.class);
        final EventSourceDefinition eventSourceDefinition = mock(EventSourceDefinition.class);
        final Location location = mock(Location.class);

        when(qualifierAnnotationExtractor.getFrom(injectionPoint, EventSourceName.class)).thenReturn(eventSourceNameAnnotation);
        when(eventSourceNameAnnotation.value()).thenReturn(eventSourceName);
        when(eventSourceDefinitionRegistry.getEventSourceDefinitionFor(eventSourceName)).thenReturn(Optional.of(eventSourceDefinition));
        when(eventSourceDefinition.getLocation()).thenReturn(location);
        when(location.getDataSource()).thenReturn(empty());
        when(eventSourceDefinition.getName()).thenReturn(dataSourceName);

        try {
            linkedEventSourceProducer.linkedEventSource(injectionPoint);
            fail();
        } catch (final CreationException expected) {
            assertThat(expected.getMessage(), is("No DataSource specified for EventSource 'my-data-source' specified in event-sources.yaml"));
        }

        verifyNoInteractions(jdbcLinkedEventSourceFactory);
    }
}
