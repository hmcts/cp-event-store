package uk.gov.justice.services.eventstore.management.catchup.process;

import uk.gov.justice.services.event.sourcing.subscription.manager.LinkedEventSourceProvider;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventsourcing.source.api.service.core.LinkedEventSource;
import uk.gov.justice.services.subscription.ProcessedEventTrackingService;

import java.util.stream.Stream;

import javax.inject.Inject;

public class MissingEventStreamer {

    @Inject
    private LinkedEventSourceProvider linkedEventSourceProvider;

    @Inject
    private ProcessedEventTrackingService processedEventTrackingService;

    public Stream<LinkedEvent> getMissingEvents(
            final String eventSourceName,
            final String componentName,
            final Long runFromEventNumber) {

        final LinkedEventSource linkedEventSource = linkedEventSourceProvider.getLinkedEventSource(eventSourceName);
        final Long highestPublishedEventNumber = linkedEventSource.getHighestPublishedEventNumber();

        return processedEventTrackingService.getAllMissingEvents(
                        eventSourceName,
                        componentName,
                        runFromEventNumber,
                        highestPublishedEventNumber)
                .flatMap(linkedEventSource::findEventRange);
    }
}
