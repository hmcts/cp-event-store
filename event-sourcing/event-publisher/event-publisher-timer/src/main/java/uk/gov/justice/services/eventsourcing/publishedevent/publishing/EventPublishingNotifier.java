package uk.gov.justice.services.eventsourcing.publishedevent.publishing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.services.eventsourcing.publishedevent.publish.PublishedEventDeQueuerAndPublisher;

import javax.annotation.Resource;
import javax.ejb.Singleton;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicBoolean;

@Singleton
public class EventPublishingNotifier {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventPublishingNotifier.class);

    @Inject
    private PublishedEventDeQueuerAndPublisher publishedEventDeQueuerAndPublisher;

    @Inject
    private PublisherTimerConfig publisherTimerConfig;

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
        long currentBackoff = publisherTimerConfig.getBackoffMinMilliseconds();

        while (!Thread.currentThread().isInterrupted()) {
            try {
                if (publishedEventDeQueuerAndPublisher.deQueueAndPublish()) {
                    currentBackoff = publisherTimerConfig.getBackoffMinMilliseconds();
                } else {
                    synchronized (monitor) {
                        monitor.wait(currentBackoff);
                    }
                    currentBackoff = Math.min(
                            (long) (currentBackoff * publisherTimerConfig.getBackoffMultiplier()),
                            publisherTimerConfig.getBackoffMaxMilliseconds()
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
                        (long) (currentBackoff * publisherTimerConfig.getBackoffMultiplier()),
                        publisherTimerConfig.getBackoffMaxMilliseconds()
                );
            }
        }
    }
}
