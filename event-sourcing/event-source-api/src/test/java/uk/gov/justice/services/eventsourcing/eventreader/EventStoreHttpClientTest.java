package uk.gov.justice.services.eventsourcing.eventreader;

import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.clients.core.HttpCaller;
import uk.gov.justice.services.clients.core.HttpCallerResponse;
import uk.gov.justice.services.eventsourcing.eventreader.EventStoreHttpClient;
import uk.gov.justice.services.eventsourcing.eventreader.RestNextEventReaderException;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.JsonObjectEnvelopeConverter;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EventStoreHttpClientTest {

    private static final String REST_URI = "http://localhost:8080/some-context";

    @Mock
    private HttpCaller defaultHttpCaller;

    @Mock
    private JsonObjectEnvelopeConverter jsonObjectEnvelopeConverter;

    @InjectMocks
    private EventStoreHttpClient eventStoreHttpClient;

    @Test
    public void shouldReturnJsonEnvelopeWhenEventFound() {

        final UUID streamId = randomUUID();
        final Long afterPosition = 5L;
        final String responseBody = "{\"_metadata\":{\"id\":\"" + randomUUID() + "\",\"name\":\"some.event\"}}";
        final JsonEnvelope jsonEnvelope = mock(JsonEnvelope.class);
        final String expectedUrl = REST_URI + "/event/" + streamId + "?afterPosition=" + afterPosition;

        when(defaultHttpCaller.get(expectedUrl, Map.of("Accept", "application/json")))
                .thenReturn(new HttpCallerResponse(200, responseBody));
        when(jsonObjectEnvelopeConverter.asEnvelope(responseBody)).thenReturn(jsonEnvelope);

        final Optional<JsonEnvelope> result = eventStoreHttpClient.getNextEvent(REST_URI, streamId, afterPosition);

        assertThat(result, is(of(jsonEnvelope)));
    }

    @Test
    public void shouldReturnEmptyWhenNoContentResponse() {

        final UUID streamId = randomUUID();
        final Long afterPosition = 5L;
        final String expectedUrl = REST_URI + "/event/" + streamId + "?afterPosition=" + afterPosition;

        when(defaultHttpCaller.get(expectedUrl, Map.of("Accept", "application/json")))
                .thenReturn(new HttpCallerResponse(204, null));

        final Optional<JsonEnvelope> result = eventStoreHttpClient.getNextEvent(REST_URI, streamId, afterPosition);

        assertThat(result.isPresent(), is(false));
    }

    @Test
    public void shouldThrowExceptionWhenResponseIsNotSuccessful() {

        final UUID streamId = randomUUID();
        final Long afterPosition = 5L;
        final String expectedUrl = REST_URI + "/event/" + streamId + "?afterPosition=" + afterPosition;

        when(defaultHttpCaller.get(expectedUrl, Map.of("Accept", "application/json")))
                .thenReturn(new HttpCallerResponse(500, null));

        assertThrows(
                RestNextEventReaderException.class,
                () -> eventStoreHttpClient.getNextEvent(REST_URI, streamId, afterPosition));
    }
}
