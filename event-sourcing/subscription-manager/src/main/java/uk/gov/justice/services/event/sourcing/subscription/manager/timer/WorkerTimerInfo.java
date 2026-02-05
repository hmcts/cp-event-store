package uk.gov.justice.services.event.sourcing.subscription.manager.timer;

import uk.gov.justice.subscription.SourceComponentPair;

import java.io.Serializable;

public record WorkerTimerInfo(SourceComponentPair sourceComponentPair, int workerNumber) implements Serializable {
}
