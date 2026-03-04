package uk.gov.justice.services.event.sourcing.subscription.manager;

public class StreamSessionLockException extends RuntimeException {

    public StreamSessionLockException(final String message) {
        super(message);
    }

    public StreamSessionLockException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
