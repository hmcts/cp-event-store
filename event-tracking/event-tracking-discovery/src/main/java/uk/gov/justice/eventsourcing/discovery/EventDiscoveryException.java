package uk.gov.justice.eventsourcing.discovery;

public class EventDiscoveryException extends RuntimeException {
    public EventDiscoveryException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
