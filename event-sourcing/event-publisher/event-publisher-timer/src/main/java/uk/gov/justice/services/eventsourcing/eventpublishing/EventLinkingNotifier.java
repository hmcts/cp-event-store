package uk.gov.justice.services.eventsourcing.eventpublishing;

import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.EventDetailsToLink;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventAppendedEvent;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventLinkedEvent;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Resource;
import javax.ejb.Singleton;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.enterprise.event.Event;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.slf4j.Logger;

@Singleton
public class EventLinkingNotifier {

    private static final Object SIGNAL = new Object();

    @Inject
    private Logger logger;

    @Inject
    private EventNumberLinker eventNumberLinker;

    @Inject
    private EventPublishingNotifier eventPublishingNotifier;

    @Inject
    private Event<EventLinkedEvent> eventLinkedEventFirer;

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
                    List<EventDetailsToLink> linkedEvents;
                    while (!(linkedEvents = eventNumberLinker.findAndLinkEventsInBatch()).isEmpty()) {
                        notifyListeners(linkedEvents);
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

    private void notifyListeners(final List<EventDetailsToLink> linkedEvents) {
        eventPublishingNotifier.wakeUp(false);

        final Map<UUID, Long> streamPositions = new HashMap<>();
        for (final EventDetailsToLink details : linkedEvents) {
            streamPositions.merge(details.streamId(), details.positionInStream(), Math::max);
        }
        eventLinkedEventFirer.fireAsync(new EventLinkedEvent(Map.copyOf(streamPositions)));
    }
}