package uk.gov.justice.services.eventsourcing.eventpublishing;

import javax.ejb.Timer;

public class SufficientTimeRemainingCalculatorFactory {

    public SufficientTimeRemainingCalculator createNew(final Timer timer, final Long timeBetweenRunsMillis) {
        return new SufficientTimeRemainingCalculator(timer, timeBetweenRunsMillis);
    }
}
