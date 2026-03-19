package uk.gov.justice.services.eventsourcing.eventpublishing;

import uk.gov.justice.services.eventsourcing.eventpublishing.configuration.EventLinkingWorkerConfig;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventAppendedEvent;

import javax.annotation.Resource;
import javax.ejb.Singleton;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;

@Singleton
public class EventLinkingNotifier {

    @Inject
    private Logger logger;

    @Inject
    private EventNumberLinker eventNumberLinker;

    @Inject
    private EventLinkingWorkerConfig eventLinkingWorkerConfig;

    @Inject
    private EventPublishingNotifier eventPublishingNotifier;

    @Resource
    private ManagedExecutorService managedExecutorService;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final Object monitor = new Object();

    public void onEventAppendedEvent(@Observes final EventAppendedEvent eventAppendedEvent) {
        this.wakeUp(false);
    }

    public void wakeUp(final boolean startIfStopped) {
        if (startIfStopped && started.compareAndSet(false, true)) {
            try {
                managedExecutorService.submit(this::runWithInterruptable);
            } catch (final Exception e) {
                started.set(false);
                logger.error("Failed to start event linking notifier thread", e);
            }
        }
        synchronized (monitor) {
            monitor.notifyAll();
        }
    }

    private void runWithInterruptable() {
        try {
            long currentBackoff = eventLinkingWorkerConfig.getBackoffMinMilliseconds();

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    if (eventNumberLinker.findAndLinkEventsInBatch() > 0) {
                        currentBackoff = eventLinkingWorkerConfig.getBackoffMinMilliseconds();
                        eventPublishingNotifier.wakeUp(false);
                    } else {
                        synchronized (monitor) {
                            monitor.wait(currentBackoff);
                        }
                        currentBackoff = Math.min(
                                (long) (currentBackoff * eventLinkingWorkerConfig.getBackoffMultiplier()),
                                eventLinkingWorkerConfig.getBackoffMaxMilliseconds()
                        );
                    }
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (final Exception e) {
                    logger.error("Error in event linking notifier loop", e);
                    try {
                        synchronized (monitor) {
                            monitor.wait(currentBackoff);
                        }
                    } catch (final InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                    currentBackoff = Math.min(
                            (long) (currentBackoff * eventLinkingWorkerConfig.getBackoffMultiplier()),
                            eventLinkingWorkerConfig.getBackoffMaxMilliseconds()
                    );
                }
            }
        } finally {
            started.set(false);
            logger.info("Event linking notifier thread exited, will restart on next timer tick");
        }
    }
}