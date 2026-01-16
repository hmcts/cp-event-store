package uk.gov.justice.services.eventsourcing.repository.jdbc.discovery;

public class EventStoreEventDiscoveryException extends RuntimeException {

    public EventStoreEventDiscoveryException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
