package uk.gov.justice.services.eventsourcing.eventpublishing;

import uk.gov.justice.services.eventsourcing.eventpublishing.configuration.EventPublishingWorkerConfig;

import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Resource;
import javax.ejb.Singleton;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.inject.Inject;

import org.slf4j.Logger;

@Singleton
public class EventPublishingNotifier {

    @Inject
    private Logger logger;

    @Inject
    private LinkedEventPublisher linkedEventPublisher;

    @Inject
    private EventPublishingWorkerConfig eventPublishingWorkerConfig;

    @Resource
    private ManagedExecutorService managedExecutorService;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final Object monitor = new Object();

    public void wakeUp(final boolean startIfStopped) {
        if (startIfStopped && started.compareAndSet(false, true)) {
            try {
                managedExecutorService.submit(this::runWithInterruptable);
            } catch (final Exception e) {
                started.set(false);
                logger.error("Failed to start event publishing notifier thread", e);
            }
        }
        synchronized (monitor) {
            monitor.notifyAll();
        }
    }

    private void runWithInterruptable() {
        try {
            long currentBackoff = eventPublishingWorkerConfig.getBackoffMinMilliseconds();

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    if (linkedEventPublisher.publishNextNewEvent()) {
                        currentBackoff = eventPublishingWorkerConfig.getBackoffMinMilliseconds();
                    } else {
                        synchronized (monitor) {
                            monitor.wait(currentBackoff);
                        }
                        currentBackoff = Math.min(
                                (long) (currentBackoff * eventPublishingWorkerConfig.getBackoffMultiplier()),
                                eventPublishingWorkerConfig.getBackoffMaxMilliseconds()
                        );
                    }
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (final Exception e) {
                    logger.error("Error in event publishing notifier loop", e);
                    try {
                        synchronized (monitor) {
                            monitor.wait(currentBackoff);
                        }
                    } catch (final InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                    currentBackoff = Math.min(
                            (long) (currentBackoff * eventPublishingWorkerConfig.getBackoffMultiplier()),
                            eventPublishingWorkerConfig.getBackoffMaxMilliseconds()
                    );
                }
            }
        } finally {
            started.set(false);
            logger.info("Event publishing notifier thread exited, will restart on next timer tick");
        }
    }
}
