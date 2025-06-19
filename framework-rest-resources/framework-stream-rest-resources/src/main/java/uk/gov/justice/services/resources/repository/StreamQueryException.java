package uk.gov.justice.services.resources.repository;

public class StreamQueryException extends RuntimeException {

    public StreamQueryException(final String message) {
        super(message);
    }

    public StreamQueryException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
