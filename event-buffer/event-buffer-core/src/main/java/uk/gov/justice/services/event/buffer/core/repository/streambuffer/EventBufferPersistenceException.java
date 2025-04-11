package uk.gov.justice.services.event.buffer.core.repository.streambuffer;

public class EventBufferPersistenceException extends RuntimeException {

    public EventBufferPersistenceException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
