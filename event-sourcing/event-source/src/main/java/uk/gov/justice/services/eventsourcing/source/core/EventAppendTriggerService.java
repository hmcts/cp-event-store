package uk.gov.justice.services.eventsourcing.source.core;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Long.parseLong;

import org.slf4j.Logger;
import uk.gov.justice.services.common.configuration.Value;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventAppendedEvent;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Event;
import javax.inject.Inject;
import javax.transaction.Synchronization;
import javax.transaction.TransactionSynchronizationRegistry;

/**
 * Service that handles the creation of System level events.
 */
@ApplicationScoped
public class EventAppendTriggerService {

    private final ThreadLocal<Boolean> transactionAppendTracker = new ThreadLocal<>();

    @Inject
    private TransactionSynchronizationRegistry transactionSynchronizationRegistry;

    @Inject
    private Event<EventAppendedEvent> eventAppendedEventFirer;

    @Inject
    private Logger logger;

    @Inject
    @Value(key = "event.linking.worker.notified", defaultValue = "false")
    private String eventLinkerNotified;

    private boolean shouldWorkerNotified;

    @PostConstruct
    public void postConstruct() {
        this.shouldWorkerNotified = parseBoolean(eventLinkerNotified);
    }

    private boolean shouldEventLinkerNotified() {
        return shouldWorkerNotified;
    }

    public void registerTransactionListener() {
        try {
            if (shouldEventLinkerNotified() && transactionAppendTracker.get() == null) {
                transactionSynchronizationRegistry.registerInterposedSynchronization(new EventAppendSynchronization());
                transactionAppendTracker.set(true);
            }
        } catch (Exception e) {
            logger.warn("Failed to register transaction synchronization " + transactionSynchronizationRegistry.getTransactionKey(), e);
        }
    }


    private class EventAppendSynchronization implements Synchronization {
        @Override
        public void beforeCompletion() {
            // No-op
        }

        @Override
        public void afterCompletion(int status) {
            try {
                if (status == javax.transaction.Status.STATUS_COMMITTED) {
                    eventAppendedEventFirer.fire(new EventAppendedEvent());
                }
            } finally {
                transactionAppendTracker.remove();
            }
        }
    }
}
