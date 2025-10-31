package uk.gov.justice.services.eventsourcing.eventpublishing;

import static javax.transaction.Transactional.TxType.REQUIRES_NEW;

import uk.gov.justice.services.eventsourcing.eventpublishing.configuration.EventLinkingWorkerConfig;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.AdvisoryLockDataAccess;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.CompatibilityModePublishedEventRepository;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.LinkEventsInEventLogDatabaseAccess;

import java.util.Optional;
import java.util.UUID;

import javax.inject.Inject;
import javax.transaction.Transactional;

public class EventNumberLinker {

    static final Long ADVISORY_LOCK_KEY = 42L;

    @Inject
    private LinkEventsInEventLogDatabaseAccess linkEventsInEventLogDatabaseAccess;

    @Inject
    private AdvisoryLockDataAccess advisoryLockDataAccess;

    @Inject
    private CompatibilityModePublishedEventRepository compatibilityModePublishedEventRepository;

    @Inject
    private EventLinkingWorkerConfig eventLinkingWorkerConfig;

    @Transactional(REQUIRES_NEW)
    public boolean findAndAndLinkNextUnlinkedEvent() {

        // obtain advisory lock if available
        if(advisoryLockDataAccess.tryNonBlockingTransactionLevelAdvisoryLock(ADVISORY_LOCK_KEY)) {
            final Optional<UUID> idOfNextEventToLink = linkEventsInEventLogDatabaseAccess.findIdOfNextEventToLink();
            if (idOfNextEventToLink.isPresent()) {
                final Long previousEventNumber = linkEventsInEventLogDatabaseAccess.findCurrentHighestEventNumberInEventLogTable();
                final Long newEventNumber = previousEventNumber + 1;
                final UUID eventId = idOfNextEventToLink.get();

                linkEventsInEventLogDatabaseAccess.linkEvent(eventId, newEventNumber, previousEventNumber);
                linkEventsInEventLogDatabaseAccess.insertLinkedEventIntoPublishQueue(eventId);

                // Temporary. To be removed once the migration to the new publishing is released
                // and published_event table is deleted
                if(eventLinkingWorkerConfig.shouldAlsoInsertEventIntoPublishedEventTable()) {
                    compatibilityModePublishedEventRepository.insertIntoPublishedEvent(eventId, newEventNumber, previousEventNumber);
                    compatibilityModePublishedEventRepository.setEventNumberSequenceTo(newEventNumber);
                }

                return true;
            }
        }

        return false;
    }
}
