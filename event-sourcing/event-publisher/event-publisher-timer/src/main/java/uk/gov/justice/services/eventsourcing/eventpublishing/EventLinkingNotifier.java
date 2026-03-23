package uk.gov.justice.services.eventsourcing.eventpublishing;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventAppendedEvent;

import javax.annotation.Resource;
import javax.ejb.Singleton;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.slf4j.Logger;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

@Singleton
public class EventLinkingNotifier {

    private static final Object SIGNAL = new Object();

    @Inject
    private Logger logger;

    @Inject
    private EventNumberLinker eventNumberLinker;

    @Inject
    private EventPublishingNotifier eventPublishingNotifier;

    @Resource
    private ManagedExecutorService managedExecutorService;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final ArrayBlockingQueue<Object> workSignal = new ArrayBlockingQueue<>(1);

    public void onEventAppendedEvent(@Observes final EventAppendedEvent eventAppendedEvent) {
        wakeUp(false);
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
        workSignal.offer(SIGNAL);
    }

    private void runWithInterruptable() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    workSignal.take();
                    while (eventNumberLinker.findAndLinkEventsInBatch() > 0) {
                        eventPublishingNotifier.wakeUp(false);
                    }
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (final Exception e) {
                    logger.error("Error in event linking notifier loop", e);
                }
            }
        } finally {
            started.set(false);
            logger.info("Event linking notifier thread exited, will restart on next timer tick");
        }
    }
}
