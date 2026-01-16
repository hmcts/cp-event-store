package uk.gov.justice.services.eventsourcing.discovery;

import static java.lang.Integer.parseInt;
import static java.lang.String.format;

import uk.gov.justice.services.common.configuration.Value;
import uk.gov.justice.services.eventsourcing.repository.jdbc.discovery.EventStoreEventDiscoveryException;

import javax.inject.Inject;

public class EventDiscoveryConfig {

    @Inject
    @Value(key = "event.discovery.batch.size", defaultValue = "7250")
    private String batchSize;

    public int getBatchSize() {
        try {
            return parseInt(batchSize);
        } catch (final NumberFormatException e) {
            throw new EventStoreEventDiscoveryException(format("'event.discovery.batch.size' jndi value is not an integer. Was '%s'", batchSize), e);
        }
    }
}
