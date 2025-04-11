package uk.gov.justice.services.event.buffer.core.repository.subscription;

public class TransactionException extends RuntimeException {
    public TransactionException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
