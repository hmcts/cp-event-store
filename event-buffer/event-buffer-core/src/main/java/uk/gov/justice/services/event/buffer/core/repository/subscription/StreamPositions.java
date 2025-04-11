package uk.gov.justice.services.event.buffer.core.repository.subscription;

public record StreamPositions(long incomingEventPosition, long currentStreamPosition, long lastKnownStreamPosition) {
}
