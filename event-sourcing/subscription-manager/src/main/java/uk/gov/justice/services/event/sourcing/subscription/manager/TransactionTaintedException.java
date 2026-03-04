package uk.gov.justice.services.event.sourcing.subscription.manager;

public class TransactionTaintedException extends RuntimeException {
    public TransactionTaintedException(final String message) {
        super(message);
    }
}