package uk.gov.justice.services.eventsourcing.eventpublishing;

import static java.lang.String.format;
import static javax.transaction.Transactional.TxType.REQUIRES_NEW;

import uk.gov.justice.services.eventsourcing.eventpublishing.configuration.EventPublishingWorkerConfig;
import uk.gov.justice.services.eventsourcing.publishedevent.EventPublishingException;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.CompatibilityModePublishedEventRepository;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.EventPublishingRepository;
import uk.gov.justice.services.eventsourcing.publisher.jms.EventPublisher;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.MissingEventNumberException;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.Optional;
import java.util.UUID;

import javax.inject.Inject;
import javax.transaction.Transactional;

public class LinkedEventPublisher {

    @Inject
    private EventPublisher eventPublisher;

    @Inject
    private EventPublishingRepository eventPublishingRepository;

    @Inject
    private LinkedJsonEnvelopeCreator linkedJsonEnvelopeCreator;

    @Inject
    private EventPublishingWorkerConfig eventPublishingWorkerConfig;

    @Inject
    private CompatibilityModePublishedEventRepository compatibilityModePublishedEventRepository;

    @Transactional(REQUIRES_NEW)
    public boolean publishNextNewEvent() {

        final Optional<UUID> eventId = eventPublishingRepository.popNextEventIdFromPublishQueue();
        if (eventId.isPresent()) {
            final Optional<LinkedEvent> linkedEventOptional = eventPublishingRepository.findEventFromEventLog(eventId.get());

            if (linkedEventOptional.isPresent()) {
                final LinkedEvent linkedEvent = linkedEventOptional.get();
                final JsonEnvelope linkedJsonEnvelope = linkedJsonEnvelopeCreator.createLinkedJsonEnvelopeFrom(linkedEvent);
                eventPublisher.publish(linkedJsonEnvelope);
                eventPublishingRepository.setIsPublishedFlag(eventId.get(), true);

                // Temporary. To be removed once the migration to the new publishing is released
                // and published_event table is deleted
                if(eventPublishingWorkerConfig.shouldAlsoInsertEventIntoPublishedEventTable()) {
                    compatibilityModePublishedEventRepository.insertIntoPublishedEvent(linkedJsonEnvelope);
                    final Long eventNumber = linkedEvent
                            .getEventNumber()
                            .orElseThrow(() -> new MissingEventNumberException(format("Event with id '%s' has null event_number in event_log table", eventId.get())));
                    compatibilityModePublishedEventRepository.setEventNumberSequenceTo(eventNumber);
                }
                
                return true;

            } else {
                throw new EventPublishingException(format("Failed to find LinkedEvent in event_log with id '%s' when id exists in publish_queue table", eventId.get()));
            }
        }

        return false;
    }
}
