package uk.gov.justice.eventsourcing.discovery;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.clients.core.HttpCallerResponse;
import uk.gov.justice.services.clients.core.httpclient.DefaultHttpCaller;
import uk.gov.justice.services.eventsourcing.discovery.DiscoveryResult;
import uk.gov.justice.services.eventsourcing.repository.jdbc.discovery.EventStoreEventDiscoveryException;

import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EventDiscoveryHttpClientTest {

    private static final String REST_URI = "http://localhost:8080/some-context";

    @Mock
    private DefaultHttpCaller defaultHttpCaller;

    @Mock
    private ObjectMapper objectMapper;

    @InjectMocks
    private EventDiscoveryHttpClient eventDiscoveryHttpClient;

    @Test
    public void shouldReturnDiscoveryResultWhenAfterEventIdIsPresent() throws Exception {

        final UUID afterEventId = randomUUID();
        final int batchSize = 100;
        final String responseBody = "some-json";
        final DiscoveryResult discoveryResult = mock(DiscoveryResult.class);
        final String expectedUrl = REST_URI + "/events-discovery?batchSize=" + batchSize + "&afterEventId=" + afterEventId;

        when(defaultHttpCaller.get(expectedUrl, Map.of("Accept", "application/json")))
                .thenReturn(new HttpCallerResponse(200, responseBody));
        when(objectMapper.readValue(responseBody, DiscoveryResult.class)).thenReturn(discoveryResult);

        final DiscoveryResult result = eventDiscoveryHttpClient.discoverEvents(REST_URI, of(afterEventId), batchSize);

        assertThat(result, is(discoveryResult));
    }

    @Test
    public void shouldReturnDiscoveryResultWhenAfterEventIdIsEmpty() throws Exception {

        final int batchSize = 100;
        final String responseBody = "some-json";
        final DiscoveryResult discoveryResult = mock(DiscoveryResult.class);
        final String expectedUrl = REST_URI + "/events-discovery?batchSize=" + batchSize;

        when(defaultHttpCaller.get(expectedUrl, Map.of("Accept", "application/json")))
                .thenReturn(new HttpCallerResponse(200, responseBody));
        when(objectMapper.readValue(responseBody, DiscoveryResult.class)).thenReturn(discoveryResult);

        final DiscoveryResult result = eventDiscoveryHttpClient.discoverEvents(REST_URI, empty(), batchSize);

        assertThat(result, is(discoveryResult));
    }

    @Test
    public void shouldThrowExceptionWhenResponseIsNotSuccessful() {

        final int batchSize = 100;
        final String expectedUrl = REST_URI + "/events-discovery?batchSize=" + batchSize;

        when(defaultHttpCaller.get(expectedUrl, Map.of("Accept", "application/json")))
                .thenReturn(new HttpCallerResponse(500, null));

        assertThrows(
                EventStoreEventDiscoveryException.class,
                () -> eventDiscoveryHttpClient.discoverEvents(REST_URI, empty(), batchSize));
    }

    @Test
    public void shouldThrowExceptionWhenJsonParsingFails() throws Exception {

        final int batchSize = 100;
        final String responseBody = "some-json";
        final String expectedUrl = REST_URI + "/events-discovery?batchSize=" + batchSize;

        when(defaultHttpCaller.get(expectedUrl, Map.of("Accept", "application/json")))
                .thenReturn(new HttpCallerResponse(200, responseBody));
        when(objectMapper.readValue(responseBody, DiscoveryResult.class))
                .thenThrow(new JsonProcessingException("parse error") {});

        assertThrows(
                EventStoreEventDiscoveryException.class,
                () -> eventDiscoveryHttpClient.discoverEvents(REST_URI, empty(), batchSize));
    }
}
