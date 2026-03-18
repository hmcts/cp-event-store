package uk.gov.justice.services.eventsourcing.publishedevent.prepublish;

import org.slf4j.Logger;
import uk.gov.justice.services.eventsourcing.publishedevent.publishing.EventPublishingNotifier;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventAppendedEvent;

import javax.annotation.Resource;
import javax.ejb.Singleton;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicBoolean;

@Singleton
public class PrePublishNotifier {

    @Inject
    private Logger logger;

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
            try {
                managedExecutorService.submit(this::runWithInterruptable);
            } catch (final Exception e) {
                started.set(false);
                logger.error("Failed to start pre-publish notifier thread", e);
            }
        }
        synchronized (monitor) {
            monitor.notifyAll();
        }
    }

    private void runWithInterruptable() {
        try {
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
                    break;
                } catch (Exception e) {
                    logger.error("Error in pre-publish notifier loop", e);
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
        } finally {
            started.set(false);
            logger.info("Pre-publish notifier thread exited, will restart on next timer tick");
        }
    }
}
