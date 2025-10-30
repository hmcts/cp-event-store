package uk.gov.justice.services.eventsourcing.source.core;

import static java.lang.String.format;
import static java.util.UUID.randomUUID;
import static uk.gov.justice.domain.annotation.Event.SYSTEM_EVENTS;
import static uk.gov.justice.services.messaging.JsonObjects.getJsonBuilderFactory;

import uk.gov.justice.services.common.util.Clock;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.Metadata;

import java.util.UUID;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.json.JsonObject;

/**
 * Service that handles the creation of System level events.
 */
@ApplicationScoped
public class SystemEventService {

    private static final String SYSTEM_EVENT_PATTERN = SYSTEM_EVENTS + "%s";
    private static final String SYSTEM_USER = "system";

    @Inject
    private Clock clock;

    public JsonEnvelope clonedEventFor(final UUID streamId) {
        final Metadata metadata = JsonEnvelope
                .metadataBuilder()
                .withId(randomUUID())
                .withName(format(SYSTEM_EVENT_PATTERN, "cloned"))
                .withUserId(SYSTEM_USER)
                .createdAt(clock.now())
                .build();

        final JsonObject payload = getJsonBuilderFactory().createObjectBuilder()
                .add("originatingStream", streamId.toString())
                .add("operation", "cloned")
                .build();

        return JsonEnvelope.envelopeFrom(metadata, payload);
    }
}
