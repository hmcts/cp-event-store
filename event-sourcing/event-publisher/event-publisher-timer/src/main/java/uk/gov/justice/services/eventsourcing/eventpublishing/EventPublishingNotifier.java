package uk.gov.justice.services.eventsourcing.eventpublishing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.services.eventsourcing.eventpublishing.configuration.EventPublishingWorkerConfig;

import javax.annotation.Resource;
import javax.ejb.Singleton;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicBoolean;

@Singleton
public class EventPublishingNotifier {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventPublishingNotifier.class);

    @Inject
    private LinkedEventPublisher linkedEventPublisher;

    @Inject
    private EventPublishingWorkerConfig eventPublishingWorkerConfig;

    @Resource
    private ManagedExecutorService managedExecutorService;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final Object monitor = new Object();

    public void wakeUp(boolean startIfStopped) {
        if (startIfStopped && started.compareAndSet(false, true)) {
            managedExecutorService.submit(this::runWithInterruptable);
        }
        synchronized (monitor) {
            monitor.notifyAll();
        }
    }

    private void runWithInterruptable() {
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
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.info("EventPublishingWorker interrupted, stopping.");
                break;
            } catch (Exception e) {
                LOGGER.error("Error in EventPublishingWorker loop", e);
                // Ensure we don't spin in a tight loop on error
                try {
                    synchronized (monitor) {
                        monitor.wait(currentBackoff);
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
                currentBackoff = Math.min(
                        (long) (currentBackoff * eventPublishingWorkerConfig.getBackoffMultiplier()),
                        eventPublishingWorkerConfig.getBackoffMaxMilliseconds()
                );
            }
        }
    }
}
