package uk.gov.justice.services.eventsourcing.publishedevent;

public class EventPublishingException extends RuntimeException {

    public EventPublishingException(final String message) {
        super(message);
    }

    public EventPublishingException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
