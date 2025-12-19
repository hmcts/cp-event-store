package uk.gov.justice.services.eventsourcing.publishedevent.prepublish;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.services.eventsourcing.publishedevent.publishing.EventPublishingNotifier;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventAppendedEvent;

import javax.annotation.Resource;
import javax.ejb.Singleton;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicBoolean;

@Singleton
public class EventLinkingNotifier {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventLinkingNotifier.class);

    @Inject
    private PrePublishProcessor prePublishProcessor;

    @Inject
    private PrePublisherTimerConfig prePublisherTimerConfig;

    @Inject
    private EventPublishingNotifier eventPublishingNotifier;

    @Resource
    private ManagedExecutorService managedExecutorService;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final Object monitor = new Object();


    public void onEventAppendedEvent(@Observes final EventAppendedEvent eventAppendedEvent) {
        this.wakeUp(false);
    }

    public void wakeUp(boolean startIfStopped) {
        if (startIfStopped && started.compareAndSet(false, true)) {
            managedExecutorService.submit(this::runWithInterruptable);
        }
        synchronized (monitor) {
            monitor.notifyAll();
        }
    }

    private void runWithInterruptable() {
        long currentBackoff = prePublisherTimerConfig.getBackoffMinMilliseconds();

        while (!Thread.currentThread().isInterrupted()) {
            try {
                if (prePublishProcessor.prePublishNextEvent()) {
                    currentBackoff = prePublisherTimerConfig.getBackoffMinMilliseconds();
                    eventPublishingNotifier.wakeUp(false);
                } else {
                    synchronized (monitor) {
                        monitor.wait(currentBackoff);
                    }
                    currentBackoff = Math.min(
                            (long) (currentBackoff * prePublisherTimerConfig.getBackoffMultiplier()),
                            prePublisherTimerConfig.getBackoffMaxMilliseconds()
                    );
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.info("EventLinkingWorker interrupted, stopping.");
                break;
            } catch (Exception e) {
                LOGGER.error("Error in EventLinkingWorker loop", e);
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
                        (long) (currentBackoff * prePublisherTimerConfig.getBackoffMultiplier()),
                        prePublisherTimerConfig.getBackoffMaxMilliseconds()
                );
            }
        }
    }
}
