package uk.gov.justice.services.eventsourcing.eventreader;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.eventsourcing.eventreader.LinkedEventMapper;
import uk.gov.justice.services.eventsourcing.eventreader.RestNextEventReaderException;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventConverter;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.messaging.JsonEnvelope;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class LinkedEventMapperTest {

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private EventConverter eventConverter;

    @InjectMocks
    private LinkedEventMapper linkedEventMapper;

    @Test
    public void shouldDeserialiseJsonStringToLinkedEventAndDelegateToEventConverter() throws Exception {

        final String jsonString = "some-json-string";
        final LinkedEvent linkedEvent = mock(LinkedEvent.class);
        final JsonEnvelope jsonEnvelope = mock(JsonEnvelope.class);

        when(objectMapper.readValue(jsonString, LinkedEvent.class)).thenReturn(linkedEvent);
        when(eventConverter.envelopeOf(linkedEvent)).thenReturn(jsonEnvelope);

        assertThat(linkedEventMapper.toJsonEnvelope(jsonString), is(jsonEnvelope));
    }

    @Test
    public void shouldThrowExceptionWhenJsonProcessingFails() throws Exception {

        final String jsonString = "some-json-string";

        when(objectMapper.readValue(jsonString, LinkedEvent.class))
                .thenThrow(new JsonProcessingException("parse error") {});

        assertThrows(
                RestNextEventReaderException.class,
                () -> linkedEventMapper.toJsonEnvelope(jsonString));
    }
}
