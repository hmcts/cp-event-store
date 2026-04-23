package uk.gov.justice.services.event.sourcing.subscription.manager;

import uk.gov.justice.services.event.sourcing.subscription.manager.timer.StreamProcessingConfig;
import uk.gov.justice.services.eventsourcing.eventreader.RestReader;
import uk.gov.justice.services.eventsourcing.eventreader.TransactionalReader;
import uk.gov.justice.services.eventsourcing.source.api.service.core.NextEventReader;

import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

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
