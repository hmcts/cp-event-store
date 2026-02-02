package uk.gov.justice.services.eventsourcing.eventpoller;

public class EventStreamPollerConfigurationException extends RuntimeException {
    public EventStreamPollerConfigurationException(final String message, final Throwable cause) {
        super(message, cause);
    }
}