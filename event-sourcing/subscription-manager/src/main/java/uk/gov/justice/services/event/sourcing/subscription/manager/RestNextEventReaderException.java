package uk.gov.justice.services.event.sourcing.subscription.manager;

public class RestNextEventReaderException extends RuntimeException {

    public RestNextEventReaderException(final String message) {
        super(message);
    }

    public RestNextEventReaderException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
