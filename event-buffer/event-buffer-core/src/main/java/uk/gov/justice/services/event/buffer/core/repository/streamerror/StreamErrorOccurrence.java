package uk.gov.justice.services.event.buffer.core.repository.streamerror;

import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;

public record StreamErrorOccurrence(
        UUID id,
        String hash,
        String exceptionMessage,
        Optional<String> causeMessage,
        String eventName,
        UUID eventId,
        UUID streamId,
        Long positionInStream,
        ZonedDateTime dateCreated,
        String fullStackTrace,
        String componentName,
        String source,
        ZonedDateTime occurredAt) {

}
