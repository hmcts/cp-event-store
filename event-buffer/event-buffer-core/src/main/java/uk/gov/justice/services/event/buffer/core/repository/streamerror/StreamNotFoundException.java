package uk.gov.justice.services.event.buffer.core.repository.streamerror;

public class StreamNotFoundException extends RuntimeException {

    public StreamNotFoundException(final String message) {
        super(message);
    }
}
