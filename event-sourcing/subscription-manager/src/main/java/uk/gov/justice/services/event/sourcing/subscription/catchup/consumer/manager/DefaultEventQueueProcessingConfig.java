package uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.manager;

import static java.lang.Integer.parseInt;

import uk.gov.justice.services.common.configuration.GlobalValue;

import jakarta.annotation.Priority;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;

@Alternative
@Priority(100)
public class DefaultEventQueueProcessingConfig implements EventQueueProcessingConfig {

    @Inject
    @GlobalValue(key = "catchup.event.processing.max.total.events.in.process", defaultValue = "100000")
    private String maxTotalEventsInProcess;

    @Override
    public int getMaxTotalEventsInProcess() {
        return parseInt(maxTotalEventsInProcess);
    }
}
