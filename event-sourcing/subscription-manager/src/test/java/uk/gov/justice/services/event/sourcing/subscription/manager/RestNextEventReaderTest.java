package uk.gov.justice.services.event.sourcing.subscription.manager;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.subscription.domain.eventsource.EventSourceDefinition;
import uk.gov.justice.subscription.domain.eventsource.Location;
import uk.gov.justice.subscription.registry.EventSourceDefinitionRegistry;

import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class RestNextEventReaderTest {

    private static final String SOURCE = "some-source";
    private static final String REST_URI = "http://localhost:8080/some-context";

    @Mock
    private EventSourceDefinitionRegistry eventSourceDefinitionRegistry;

    @Mock
    private EventStoreHttpClient eventStoreHttpClient;

    @InjectMocks
    private RestNextEventReader restNextEventReader;

    @Test
    public void shouldReturnJsonEnvelopeWhenEventIsFound() {

        final UUID streamId = randomUUID();
        final Long position = 5L;
        final JsonEnvelope jsonEnvelope = mock(JsonEnvelope.class);

        givenRestUriConfiguredFor(SOURCE, REST_URI);
        when(eventStoreHttpClient.getNextEvent(REST_URI, streamId, position)).thenReturn(of(jsonEnvelope));

        final Optional<JsonEnvelope> result = restNextEventReader.read(streamId, position, SOURCE);

        assertThat(result, is(of(jsonEnvelope)));
    }

    @Test
    public void shouldReturnEmptyWhenNoEventFound() {

        final UUID streamId = randomUUID();
        final Long position = 5L;

        givenRestUriConfiguredFor(SOURCE, REST_URI);
        when(eventStoreHttpClient.getNextEvent(REST_URI, streamId, position)).thenReturn(empty());

        final Optional<JsonEnvelope> result = restNextEventReader.read(streamId, position, SOURCE);

        assertThat(result.isPresent(), is(false));
    }

    @Test
    public void shouldThrowExceptionWhenEventSourceDefinitionNotFound() {

        final UUID streamId = randomUUID();
        final Long position = 5L;

        when(eventSourceDefinitionRegistry.getEventSourceDefinitionFor(SOURCE)).thenReturn(empty());

        assertThrows(
                RestNextEventReaderException.class,
                () -> restNextEventReader.read(streamId, position, SOURCE));
    }

    @Test
    public void shouldThrowExceptionWhenRestUriNotConfigured() {

        final UUID streamId = randomUUID();
        final Long position = 5L;
        final Location location = mock(Location.class);
        final EventSourceDefinition eventSourceDefinition = mock(EventSourceDefinition.class);

        when(eventSourceDefinitionRegistry.getEventSourceDefinitionFor(SOURCE)).thenReturn(of(eventSourceDefinition));
        when(eventSourceDefinition.getLocation()).thenReturn(location);
        when(location.getRestUri()).thenReturn(empty());

        assertThrows(
                RestNextEventReaderException.class,
                () -> restNextEventReader.read(streamId, position, SOURCE));
    }

    private void givenRestUriConfiguredFor(final String source, final String restUri) {
        final Location location = mock(Location.class);
        final EventSourceDefinition eventSourceDefinition = mock(EventSourceDefinition.class);
        when(eventSourceDefinitionRegistry.getEventSourceDefinitionFor(source)).thenReturn(of(eventSourceDefinition));
        when(eventSourceDefinition.getLocation()).thenReturn(location);
        when(location.getRestUri()).thenReturn(of(restUri));
    }
}
