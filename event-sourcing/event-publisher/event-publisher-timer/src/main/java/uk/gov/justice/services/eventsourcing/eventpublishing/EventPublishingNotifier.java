package uk.gov.justice.services.eventsourcing.eventpublishing;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.annotation.Resource;
import jakarta.ejb.Singleton;
import jakarta.enterprise.concurrent.ManagedExecutorService;
import jakarta.inject.Inject;

import org.slf4j.Logger;

@Singleton
public class EventPublishingNotifier {

    private static final Object SIGNAL = new Object();

    @Inject
    private Logger logger;

    @Inject
    private LinkedEventPublisher linkedEventPublisher;

    @Resource
    private ManagedExecutorService managedExecutorService;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final ArrayBlockingQueue<Object> workSignal = new ArrayBlockingQueue<>(1);

    public void wakeUp(final boolean startIfStopped) {
        if (startIfStopped && started.compareAndSet(false, true)) {
            try {
                managedExecutorService.submit(this::runWithInterruptable);
            } catch (final Exception e) {
                started.set(false);
                logger.error("Failed to start event publishing notifier thread", e);
            }
        }
        workSignal.offer(SIGNAL);
    }

    private void runWithInterruptable() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    workSignal.take();
                    while (linkedEventPublisher.publishNextNewEvent()) {}
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (final Exception e) {
                    logger.error("Error in event publishing notifier loop", e);
                }
            }
        } finally {
            started.set(false);
            logger.info("Event publishing notifier thread exited, will restart on next timer tick");
        }
    }
}