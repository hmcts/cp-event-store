package uk.gov.justice.services.eventsourcing.publishedevent.jdbc;

public class AdvisoryLockException extends RuntimeException {

    public AdvisoryLockException(final String message) {
        super(message);
    }

    public AdvisoryLockException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
