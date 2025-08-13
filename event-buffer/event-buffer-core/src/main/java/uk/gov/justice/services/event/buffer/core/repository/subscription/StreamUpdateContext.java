package uk.gov.justice.services.event.buffer.core.repository.subscription;

import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorDetails;

import java.sql.Timestamp;
import java.util.Optional;
import java.util.UUID;

public record StreamUpdateContext(
        long incomingEventPosition,
        long currentStreamPosition,
        long latestKnownStreamPosition,
        Timestamp lastUpdatedAt,
        Optional<UUID> streamErrorId,
        Optional<StreamErrorDetails> existingStreamErrorDetails) {
}
