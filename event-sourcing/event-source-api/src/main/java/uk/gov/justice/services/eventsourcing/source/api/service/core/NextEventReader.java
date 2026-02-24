package uk.gov.justice.services.eventsourcing.source.api.service.core;

import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.Optional;
import java.util.UUID;

public interface NextEventReader {

    Optional<JsonEnvelope> read(UUID streamId, Long position);
}
