package uk.gov.justice.services.event.sourcing.subscription.manager;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventConverter;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.messaging.JsonEnvelope;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

@ApplicationScoped
public class LinkedEventMapper {

    @Inject
    private ObjectMapper objectMapper;

    @Inject
    private EventConverter eventConverter;

    public JsonEnvelope toJsonEnvelope(final String jsonString) {
        try {
            final LinkedEvent linkedEvent = objectMapper.readValue(jsonString, LinkedEvent.class);
            return eventConverter.envelopeOf(linkedEvent);
        } catch (final JsonProcessingException e) {
            throw new RestNextEventReaderException("Failed to parse event JSON: " + e.getMessage(), e);
        }
    }
}
