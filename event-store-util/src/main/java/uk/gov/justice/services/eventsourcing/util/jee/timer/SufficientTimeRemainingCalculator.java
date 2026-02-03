package uk.gov.justice.services.eventsourcing.util.jee.timer;

import javax.ejb.Timer;

public class SufficientTimeRemainingCalculator {

    private final Timer timer;
    private final Long timeBetweenRunsMillis;

    public SufficientTimeRemainingCalculator(final Timer timer, final Long timeBetweenRunsMillis) {
        this.timer = timer;
        this.timeBetweenRunsMillis = timeBetweenRunsMillis;
    }

    public boolean hasSufficientProcessingTimeRemaining() {
        return timer.getTimeRemaining() > timeBetweenRunsMillis;
    }
}
