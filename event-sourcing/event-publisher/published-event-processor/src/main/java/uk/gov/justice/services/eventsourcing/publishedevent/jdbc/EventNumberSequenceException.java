package uk.gov.justice.services.eventsourcing.publishedevent.jdbc;

public class EventNumberSequenceException extends RuntimeException {

    public EventNumberSequenceException(final String message) {
        super(message);
    }

    public EventNumberSequenceException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
