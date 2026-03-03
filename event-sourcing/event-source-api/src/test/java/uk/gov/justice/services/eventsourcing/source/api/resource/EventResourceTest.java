package uk.gov.justice.services.eventsourcing.source.api.resource;

import static java.lang.String.format;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NO_CONTENT;
import static javax.ws.rs.core.Response.Status.OK;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.eventsourcing.source.api.service.core.NextEventReader;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.JsonObjectEnvelopeConverter;

import java.util.UUID;

import javax.json.JsonObject;
import javax.ws.rs.core.Response;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class EventResourceTest {

    @Mock
    private NextEventReader nextEventReader;

    @Mock
    private JsonObjectEnvelopeConverter jsonObjectEnvelopeConverter;

    @Mock
    private Logger logger;

    @InjectMocks
    private EventResource eventResource;

    @Test
    public void shouldReturnConvertedEventOnSuccess() {

        final UUID streamId = randomUUID();
        final long afterPosition = 5L;
        final JsonEnvelope jsonEnvelope = mock(JsonEnvelope.class);
        final JsonObject jsonObject = mock(JsonObject.class);

        when(nextEventReader.read(streamId, afterPosition, null)).thenReturn(of(jsonEnvelope));
        when(jsonObjectEnvelopeConverter.fromEnvelope(jsonEnvelope)).thenReturn(jsonObject);

        try (Response response = eventResource.nextEvent(streamId, afterPosition)) {
            assertThat(response.getStatus(), is(OK.getStatusCode()));
            assertThat(response.getEntity(), is(jsonObject));
        }
    }

    @Test
    public void shouldReturn204WhenNoEventFound() {

        final UUID streamId = randomUUID();
        final long afterPosition = 5L;

        when(nextEventReader.read(streamId, afterPosition, null)).thenReturn(empty());

        try (Response response = eventResource.nextEvent(streamId, afterPosition)) {
            assertThat(response.getStatus(), is(NO_CONTENT.getStatusCode()));
            assertThat(response.getEntity(), is(nullValue()));
        }
    }

    @Test
    public void shouldReturn500WhenReadThrowsException() {

        final UUID streamId = randomUUID();
        final long afterPosition = 5L;
        final RuntimeException exception = new RuntimeException("Read failed");

        when(nextEventReader.read(streamId, afterPosition, null)).thenThrow(exception);

        try (Response response = eventResource.nextEvent(streamId, afterPosition)) {
            assertThat(response.getStatus(), is(INTERNAL_SERVER_ERROR.getStatusCode()));
        }
        verify(logger).error(format("Failed to read next event: streamId '%s', afterPosition '%d'", streamId, afterPosition), exception);
    }
}
