package uk.gov.justice.services.eventsourcing.source.api.resource;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.OK;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.common.converter.ObjectToJsonValueConverter;
import uk.gov.justice.services.eventsourcing.discovery.DiscoveryResult;
import uk.gov.justice.services.eventsourcing.discovery.EventSubscriptionDiscoverer;

import java.util.UUID;

import javax.json.JsonValue;
import javax.ws.rs.core.Response;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class EventDiscoveryResourceTest {

    @Mock
    private EventSubscriptionDiscoverer eventSubscriptionDiscoverer;

    @Mock
    private ObjectToJsonValueConverter converter;

    @Mock
    private Logger logger;

    @InjectMocks
    private EventDiscoveryResource eventDiscoveryResource;

    @Test
    public void shouldReturnConvertedDiscoveryResultOnSuccess() {

        final UUID afterEventId = randomUUID();
        final int batchSize = 100;
        final DiscoveryResult discoveryResult = new DiscoveryResult(emptyList(), of(randomUUID()));
        final JsonValue jsonValue = mock(JsonValue.class);

        when(eventSubscriptionDiscoverer.discoverNewEvents(of(afterEventId), batchSize)).thenReturn(discoveryResult);
        when(converter.convert(discoveryResult)).thenReturn(jsonValue);

        try (Response response = eventDiscoveryResource.discoverEvents(afterEventId, batchSize)) {
            assertThat(response.getStatus(), is(OK.getStatusCode()));
            assertThat(response.getEntity(), is(jsonValue));
        }
    }

    @Test
    public void shouldReturnConvertedDiscoveryResultWithNullAfterEventId() {

        final int batchSize = 50;
        final DiscoveryResult discoveryResult = new DiscoveryResult(emptyList(), empty());
        final JsonValue jsonValue = mock(JsonValue.class);

        when(eventSubscriptionDiscoverer.discoverNewEvents(empty(), batchSize)).thenReturn(discoveryResult);
        when(converter.convert(discoveryResult)).thenReturn(jsonValue);

        try (Response response = eventDiscoveryResource.discoverEvents(null, batchSize)) {
            assertThat(response.getStatus(), is(OK.getStatusCode()));
            assertThat(response.getEntity(), is(jsonValue));
        }
    }

    @Test
    public void shouldReturn500WhenDiscoveryThrowsException() {

        final UUID afterEventId = randomUUID();
        final int batchSize = 100;
        final RuntimeException exception = new RuntimeException("Discovery failed");

        when(eventSubscriptionDiscoverer.discoverNewEvents(of(afterEventId), batchSize)).thenThrow(exception);

        try (Response response = eventDiscoveryResource.discoverEvents(afterEventId, batchSize)) {
            assertThat(response.getStatus(), is(INTERNAL_SERVER_ERROR.getStatusCode()));
        }

        verify(logger).error(format("Failed to discover events: afterEventId '%s', batchSize '%d'", afterEventId, batchSize), exception);
    }
}
