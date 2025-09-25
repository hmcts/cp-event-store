package uk.gov.justice.services.eventsourcing.eventpublishing;

import static javax.transaction.Transactional.TxType.REQUIRES_NEW;

import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.EventNumberSequenceDataAccess;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.LinkEventsInEventLogDatabaseAccess;

import java.util.Optional;
import java.util.UUID;

import javax.inject.Inject;
import javax.transaction.Transactional;

public class EventNumberLinker {

    @Inject
    private LinkEventsInEventLogDatabaseAccess linkEventsInEventLogDatabaseAccess;

    @Inject
    private EventNumberSequenceDataAccess eventNumberSequenceDataAccess;

    @Transactional(REQUIRES_NEW)
    public boolean findAndAndLinkNextUnlinkedEvent() {

        final Long newEventNumber = eventNumberSequenceDataAccess.lockAndGetNextAvailableEventNumber();

        final Optional<UUID> idOfNextEventToLink = linkEventsInEventLogDatabaseAccess.findIdOfNextEventToLink();
        if (idOfNextEventToLink.isPresent()) {
            final Long previousEventNumber = linkEventsInEventLogDatabaseAccess.findCurrentHighestEventNumberInEventLogTable();
            final UUID eventId = idOfNextEventToLink.get();

            linkEventsInEventLogDatabaseAccess.linkEvent(eventId, newEventNumber, previousEventNumber);
            eventNumberSequenceDataAccess.updateNextAvailableEventNumberTo(newEventNumber + 1);
            linkEventsInEventLogDatabaseAccess.insertLinkedEventIntoPublishQueue(eventId);
        }

        return idOfNextEventToLink.isPresent();
    }
}
