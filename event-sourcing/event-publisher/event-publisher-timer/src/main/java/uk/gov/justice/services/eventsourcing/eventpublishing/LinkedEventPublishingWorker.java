package uk.gov.justice.services.eventsourcing.eventpublishing;

import uk.gov.justice.services.eventsourcing.publishedevent.publishing.PublisherTimerConfig;
import uk.gov.justice.services.eventsourcing.util.jee.timer.StopWatchFactory;

import javax.inject.Inject;

import org.apache.commons.lang3.time.StopWatch;

public class LinkedEventPublishingWorker {

    @Inject
    private PublisherTimerConfig publisherTimerConfig;

    @Inject
    private StopWatchFactory stopWatchFactory;

    @Inject
    private LinkedEventPublisher linkedEventPublisher;

    public void publishQueuedEvents() {
        final long maxRuntimeMilliseconds = publisherTimerConfig.getTimerMaxRuntimeMilliseconds();
        final StopWatch stopWatch = stopWatchFactory.createStopWatch();

        stopWatch.start();

        boolean running = true;
        while (running) {
            if (stopWatch.getTime() > maxRuntimeMilliseconds) {
                break;
            }
            running = linkedEventPublisher.publishNextQueuedEvent();
        }
    }
}
