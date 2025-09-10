package uk.gov.justice.services.eventsourcing.eventpublishing;

import static java.lang.String.format;
import static javax.transaction.Transactional.TxType.REQUIRES_NEW;

import uk.gov.justice.services.eventsourcing.publishedevent.EventPublishingException;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.EventPublishingRepository;
import uk.gov.justice.services.eventsourcing.publisher.jms.EventPublisher;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventConverter;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.PublishedEvent;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.Optional;
import java.util.UUID;

import javax.inject.Inject;
import javax.transaction.Transactional;

public class LinkedEventPublisher {

    @Inject
    private EventPublisher eventPublisher;

    @Inject
    private EventConverter eventConverter;

    @Inject
    private EventPublishingRepository eventPublishingRepository;

    @Transactional(REQUIRES_NEW)
    public boolean publishNextQueuedEvent() {

        final Optional<UUID> eventId = eventPublishingRepository.getNextEventIdFromPublishQueue();
        if (eventId.isPresent()) {
            final Optional<PublishedEvent> publishedEvent = eventPublishingRepository.findEventFromEventLog(eventId.get());

            if(publishedEvent.isPresent()) {
                final JsonEnvelope jsonEnvelope = eventConverter.envelopeOf(publishedEvent.get());
                eventPublisher.publish(jsonEnvelope);

                return true;
            } else {
                throw new EventPublishingException(format("Failed to find LinkedEvent with id '%s' when id exists in publish_queue table", eventId.get()));
            }
        }

        return false;
    }
}
