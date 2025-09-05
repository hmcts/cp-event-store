package uk.gov.justice.services.eventsourcing.publishedevent.jdbc;

public class EventNumberLinkingException extends RuntimeException{

    public EventNumberLinkingException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
