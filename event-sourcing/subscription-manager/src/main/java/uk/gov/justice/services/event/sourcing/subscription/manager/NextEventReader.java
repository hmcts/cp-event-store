package uk.gov.justice.services.event.sourcing.subscription.manager;

import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.Optional;
import java.util.UUID;

public interface NextEventReader {

    Optional<JsonEnvelope> read(UUID streamId, Long position);
}
