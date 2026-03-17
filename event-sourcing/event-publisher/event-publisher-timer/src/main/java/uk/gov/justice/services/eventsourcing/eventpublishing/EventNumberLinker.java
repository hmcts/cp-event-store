package uk.gov.justice.services.eventsourcing.eventpublishing;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.inject.Inject;
import javax.transaction.UserTransaction;
import uk.gov.justice.services.eventsourcing.eventpublishing.configuration.EventLinkingWorkerConfig;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.AdvisoryLockDataAccess;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.EventNumberLinkingException;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.LinkedEventData;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.LinkEventsInEventLogDatabaseAccess;

public class EventNumberLinker {

    static final Long ADVISORY_LOCK_KEY = 42L;
    public static final int NO_EVENT_LINKED = 0;

    @Inject
    private LinkEventsInEventLogDatabaseAccess linkEventsInEventLogDatabaseAccess;

    @Inject
    private AdvisoryLockDataAccess advisoryLockDataAccess;

    @Inject
    private EventLinkingWorkerConfig eventLinkingWorkerConfig;

    @Inject
    private UserTransaction userTransaction;

    public int findAndLinkEventsInBatch() {

        final int batchSize = eventLinkingWorkerConfig.getBatchSize();

        try (final Connection connection = linkEventsInEventLogDatabaseAccess.getEventStoreConnection()) {
            final int transactionTimeoutSeconds = eventLinkingWorkerConfig.getTransactionTimeoutSeconds();
            final int localStatementTimeoutSeconds = eventLinkingWorkerConfig.getLocalStatementTimeoutSeconds();
            userTransaction.setTransactionTimeout(transactionTimeoutSeconds);
            userTransaction.begin();

            linkEventsInEventLogDatabaseAccess.setStatementTimeoutOnCurrentTransaction(connection, localStatementTimeoutSeconds);

            if (!advisoryLockDataAccess.tryNonBlockingTransactionLevelAdvisoryLock(connection, ADVISORY_LOCK_KEY)) {
                userTransaction.rollback();
                return NO_EVENT_LINKED;
            }

            final List<UUID> eventIds = linkEventsInEventLogDatabaseAccess.findBatchOfNextEventIdsToLink(connection, batchSize);

            if (eventIds.isEmpty()) {
                userTransaction.rollback();
                return NO_EVENT_LINKED;
            }

            long eventNumber = linkEventsInEventLogDatabaseAccess.findCurrentHighestEventNumberInEventLogTable(connection);

            final List<LinkedEventData> linkDataList = new ArrayList<>(eventIds.size());
            for (final UUID eventId : eventIds) {
                final long previousEventNumber = eventNumber;
                eventNumber = previousEventNumber + 1;
                linkDataList.add(new LinkedEventData(eventId, eventNumber, previousEventNumber));
            }

            linkEventsInEventLogDatabaseAccess.linkEventsBatch(connection, linkDataList);
            linkEventsInEventLogDatabaseAccess.insertBatchIntoPublishQueue(connection, eventIds);

            userTransaction.commit();

            return eventIds.size();

        } catch (final Exception e) {
            try {
                userTransaction.rollback();
            } catch (final Exception ignored) {
                //ignore
            }
            throw new EventNumberLinkingException("Exception occurred while linking events in batch", e);
        }
    }
}