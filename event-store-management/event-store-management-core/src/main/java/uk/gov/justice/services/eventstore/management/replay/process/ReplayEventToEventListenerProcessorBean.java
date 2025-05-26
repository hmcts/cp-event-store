package uk.gov.justice.services.eventstore.management.replay.process;

import static javax.ejb.TransactionManagementType.CONTAINER;
import static javax.transaction.Transactional.TxType.NEVER;

import uk.gov.justice.services.event.sourcing.subscription.manager.PublishedEventSourceProvider;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventConverter;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.PublishedEvent;
import uk.gov.justice.services.eventsourcing.source.api.service.core.PublishedEventSource;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.UUID;

import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ejb.TransactionManagement;
import javax.inject.Inject;
import javax.transaction.Transactional;

@Stateless
@TransactionManagement(CONTAINER)
@TransactionAttribute(TransactionAttributeType.NEVER)
public class ReplayEventToEventListenerProcessorBean {

    @Inject
    private PublishedEventSourceProvider publishedEventSourceProvider;

    @Inject
    private TransactionReplayEventProcessor transactionReplayEventProcessor;

    @Inject
    private EventConverter eventConverter;


    @Transactional(NEVER)
    public void perform(final ReplayEventContext replayEventContext) {
        final String eventSourceName = replayEventContext.getEventSourceName();
        final UUID eventId = replayEventContext.getCommandRuntimeId();
        final String componentName = replayEventContext.getComponentName();

        final PublishedEvent publishedEvent = fetchPublishedEvent(eventSourceName, eventId);
        process(componentName, publishedEvent);
    }

    private PublishedEvent fetchPublishedEvent(final String eventSourceName, final UUID eventId) {
        final PublishedEventSource publishedEventSource = publishedEventSourceProvider.getPublishedEventSource(eventSourceName);

        return publishedEventSource.findByEventId(eventId)
                .orElseThrow(() -> new ReplayEventFailedException("Published event not found for given commandRuntimeId:" + eventId + " under event source name:" + eventSourceName));
    }

    private void process(final String componentName, final PublishedEvent publishedEvent) {
        final JsonEnvelope eventEnvelope = eventConverter.envelopeOf(publishedEvent);
        transactionReplayEventProcessor.process(componentName, eventEnvelope);
    }
}
