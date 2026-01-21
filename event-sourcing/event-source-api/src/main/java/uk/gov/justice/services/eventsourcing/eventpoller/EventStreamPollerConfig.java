package uk.gov.justice.services.eventsourcing.eventpoller;

import uk.gov.justice.services.common.configuration.Value;

import javax.inject.Inject;

import static java.lang.Integer.parseInt;
import static java.lang.String.format;

public class EventStreamPollerConfig {

    @Inject
    @Value(key = "event.poller.batch.size", defaultValue = "1000")
    private String batchSize;

    public int getBatchSize() {
        try {
            return parseInt(batchSize);
        } catch (final NumberFormatException e) {
            throw new EventStreamPollerConfigurationException(format("'event.poller.batch.size' jndi value is not an integer. Was '%s'", batchSize), e);
        }
    }
}
