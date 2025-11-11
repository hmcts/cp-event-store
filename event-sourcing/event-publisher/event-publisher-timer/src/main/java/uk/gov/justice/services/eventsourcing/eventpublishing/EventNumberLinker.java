package uk.gov.justice.services.eventsourcing.eventpublishing;

import java.util.Optional;
import java.util.UUID;
import javax.inject.Inject;
import javax.transaction.UserTransaction;
import uk.gov.justice.services.eventsourcing.eventpublishing.configuration.EventLinkingWorkerConfig;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.AdvisoryLockDataAccess;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.CompatibilityModePublishedEventRepository;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.EventNumberLinkingException;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.LinkEventsInEventLogDatabaseAccess;

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

    @Inject
    private UserTransaction userTransaction;

    public boolean findAndAndLinkNextUnlinkedEvent() {

        try {
            final int transactionTimeoutSeconds = eventLinkingWorkerConfig.getTransactionTimeoutSeconds();
            final int localStatementTimeoutSeconds = eventLinkingWorkerConfig.getLocalStatementTimeoutSeconds();
            userTransaction.setTransactionTimeout(transactionTimeoutSeconds);
            userTransaction.begin();
            linkEventsInEventLogDatabaseAccess.setStatementTimeoutOnCurrentTransaction(localStatementTimeoutSeconds);

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

                    userTransaction.commit();
                    return true;
                }
            }

            userTransaction.commit();
            return false;

        } catch (final Exception e) {
            try {
                userTransaction.rollback();
            } catch (final Exception ignored) {
                //ignore
            }
            throw new EventNumberLinkingException("Exception occurred while linking event number", e);
        }
    }
}
