package uk.gov.justice.services.event.buffer.core.repository.subscription;

public class StreamStatusException extends RuntimeException {

    public StreamStatusException(final String message) {
        super(message);
    }

    public StreamStatusException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
