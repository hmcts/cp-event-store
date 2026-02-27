package uk.gov.justice.services.eventsourcing.eventreader;

public class RestNextEventReaderException extends RuntimeException {

    public RestNextEventReaderException(final String message) {
        super(message);
    }

    public RestNextEventReaderException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
