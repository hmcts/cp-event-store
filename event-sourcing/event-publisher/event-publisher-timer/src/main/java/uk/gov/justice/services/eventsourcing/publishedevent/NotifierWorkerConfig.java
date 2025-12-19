package uk.gov.justice.services.eventsourcing.publishedevent;

public abstract class NotifierWorkerConfig {

    private boolean shouldWorkerNotified;

    private long backoffMinMilliseconds;

    private long backoffMaxMilliseconds;

    private double backoffMultiplier;

    public boolean shouldWorkerNotified() {
        return shouldWorkerNotified;
    }

    public long getBackoffMinMilliseconds() {
        return backoffMinMilliseconds;
    }

    public long getBackoffMaxMilliseconds() {
        return backoffMaxMilliseconds;
    }

    public double getBackoffMultiplier() {
        return backoffMultiplier;
    }

    public void setShouldWorkerNotified(final boolean shouldWorkerNotified) {
        this.shouldWorkerNotified = shouldWorkerNotified;
    }

    public void setBackoffMinMilliseconds(final long backoffMinMilliseconds) {
        this.backoffMinMilliseconds = backoffMinMilliseconds;
    }

    public void setBackoffMaxMilliseconds(final long backoffMaxMilliseconds) {
        this.backoffMaxMilliseconds = backoffMaxMilliseconds;
    }

    public void setBackoffMultiplier(final double backoffMultiplier) {
        this.backoffMultiplier = backoffMultiplier;
    }
}
