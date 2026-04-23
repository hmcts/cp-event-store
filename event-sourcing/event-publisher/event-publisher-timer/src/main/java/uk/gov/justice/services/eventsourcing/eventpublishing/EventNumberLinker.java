package uk.gov.justice.services.eventsourcing.eventpublishing;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import jakarta.inject.Inject;
import jakarta.transaction.UserTransaction;
import uk.gov.justice.services.eventsourcing.eventpublishing.configuration.EventLinkingWorkerConfig;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.AdvisoryLockDataAccess;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.EventDetailsToLink;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.EventNumberLinkingException;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.LinkedEventData;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.LinkEventsInEventLogDatabaseAccess;

public class EventNumberLinker {

    static final Long ADVISORY_LOCK_KEY = 42L;

    @Inject
    private LinkEventsInEventLogDatabaseAccess linkEventsInEventLogDatabaseAccess;

    @Inject
    private AdvisoryLockDataAccess advisoryLockDataAccess;

    @Inject
    private EventLinkingWorkerConfig eventLinkingWorkerConfig;

    @Inject
    private UserTransaction userTransaction;

    public List<EventDetailsToLink> findAndLinkEventsInBatch() {

        final int batchSize = eventLinkingWorkerConfig.getBatchSize();

        try (final Connection connection = linkEventsInEventLogDatabaseAccess.getEventStoreConnection()) {
            final int transactionTimeoutSeconds = eventLinkingWorkerConfig.getTransactionTimeoutSeconds();
            final int localStatementTimeoutSeconds = eventLinkingWorkerConfig.getLocalStatementTimeoutSeconds();
            userTransaction.setTransactionTimeout(transactionTimeoutSeconds);
            userTransaction.begin();

            linkEventsInEventLogDatabaseAccess.setStatementTimeoutOnCurrentTransaction(connection, localStatementTimeoutSeconds);

            if (!advisoryLockDataAccess.tryNonBlockingTransactionLevelAdvisoryLock(connection, ADVISORY_LOCK_KEY)) {
                userTransaction.rollback();
                return Collections.emptyList();
            }

            final List<EventDetailsToLink> events = linkEventsInEventLogDatabaseAccess.findBatchOfNextEventsToLink(connection, batchSize);

            if (events.isEmpty()) {
                userTransaction.rollback();
                return Collections.emptyList();
            }

            long eventNumber = linkEventsInEventLogDatabaseAccess.findCurrentHighestEventNumberInEventLogTable(connection);

            final List<UUID> eventIds = new ArrayList<>(events.size());
            final List<LinkedEventData> linkDataList = new ArrayList<>(events.size());
            for (final EventDetailsToLink event : events) {
                final long previousEventNumber = eventNumber;
                eventNumber = previousEventNumber + 1;
                linkDataList.add(new LinkedEventData(event.eventId(), eventNumber, previousEventNumber));
                eventIds.add(event.eventId());
            }

            linkEventsInEventLogDatabaseAccess.linkEventsBatch(connection, linkDataList);
            linkEventsInEventLogDatabaseAccess.insertBatchIntoPublishQueue(connection, eventIds);

            userTransaction.commit();

            return events;

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