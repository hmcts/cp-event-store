package uk.gov.justice.services.event.sourcing.subscription.manager;

import uk.gov.justice.services.event.sourcing.subscription.manager.timer.StreamProcessingConfig;

import javax.enterprise.inject.Produces;
import javax.inject.Inject;

public class NextEventReaderProducer {

    @Inject
    @TransactionalReader
    private NextEventReader transactionalNextEventReader;

    @Inject
    @RestReader
    private NextEventReader restNextEventReader;

    @Inject
    private StreamProcessingConfig streamProcessingConfig;

    @Produces
    public NextEventReader nextEventReader() {
        if (streamProcessingConfig.accessEventStoreViaRest()) {
            return restNextEventReader;
        }
        return transactionalNextEventReader;
    }
}
