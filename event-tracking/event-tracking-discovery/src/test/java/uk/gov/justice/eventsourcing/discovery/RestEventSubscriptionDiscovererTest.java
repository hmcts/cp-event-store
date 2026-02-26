package uk.gov.justice.eventsourcing.discovery;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.eventsourcing.discovery.DiscoveryResult;
import uk.gov.justice.subscription.registry.EventSourceDefinitionRegistry;
import uk.gov.justice.subscription.registry.RegistryException;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class RestEventSubscriptionDiscovererTest {

    private static final String SOURCE = "some-source";
    private static final String REST_URI = "http://localhost:8080/some-context";

    @Mock
    private EventSourceDefinitionRegistry eventSourceDefinitionRegistry;

    @Mock
    private EventDiscoveryHttpClient eventDiscoveryHttpClient;

    @InjectMocks
    private RestEventSubscriptionDiscoverer restEventSubscriptionDiscoverer;

    @Test
    public void shouldReturnDiscoveryResultForGivenSource() {

        final UUID afterEventId = randomUUID();
        final int batchSize = 100;
        final DiscoveryResult discoveryResult = mock(DiscoveryResult.class);

        when(eventSourceDefinitionRegistry.getRestUri(SOURCE)).thenReturn(REST_URI);
        when(eventDiscoveryHttpClient.discoverEvents(REST_URI, of(afterEventId), batchSize)).thenReturn(discoveryResult);

        final DiscoveryResult result = restEventSubscriptionDiscoverer.discoverNewEvents(of(afterEventId), batchSize, SOURCE);

        assertThat(result, is(discoveryResult));
    }

    @Test
    public void shouldThrowExceptionWhenRestUriNotConfiguredForSource() {

        when(eventSourceDefinitionRegistry.getRestUri(SOURCE))
                .thenThrow(new RegistryException("No REST URI configured for event source: " + SOURCE));

        assertThrows(
                RegistryException.class,
                () -> restEventSubscriptionDiscoverer.discoverNewEvents(empty(), 100, SOURCE));
    }
}
