package uk.gov.justice.services.event.sourcing.subscription.error;

public class StreamProcessingException extends RuntimeException{

    public StreamProcessingException(final String message) {
        super(message);
    }

    public StreamProcessingException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
