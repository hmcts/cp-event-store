package uk.gov.justice.services.eventsourcing.util.jee.timer;

import jakarta.ejb.Timer;

public class SufficientTimeRemainingCalculatorFactory {

    public SufficientTimeRemainingCalculator createNew(final Timer timer, final Long timeBetweenRunsMillis) {
        return new SufficientTimeRemainingCalculator(timer, timeBetweenRunsMillis);
    }
}
