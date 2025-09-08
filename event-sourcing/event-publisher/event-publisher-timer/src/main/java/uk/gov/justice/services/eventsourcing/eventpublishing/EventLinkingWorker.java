package uk.gov.justice.services.eventsourcing.eventpublishing;

import uk.gov.justice.services.eventsourcing.publishedevent.publishing.PublisherTimerConfig;
import uk.gov.justice.services.eventsourcing.util.jee.timer.StopWatchFactory;

import javax.inject.Inject;

import org.apache.commons.lang3.time.StopWatch;

public class EventLinkingWorker {

    @Inject
    private EventNumberLinker eventNumberLinker;

    @Inject
    private StopWatchFactory stopWatchFactory;

    @Inject
    private PublisherTimerConfig publisherTimerConfig;

    public void findAndLinkUnlinkedEvents() {

        final long maxRuntimeMilliseconds = publisherTimerConfig.getTimerMaxRuntimeMilliseconds();
        final StopWatch stopWatch = stopWatchFactory.createStopWatch();

        stopWatch.start();

        while (eventNumberLinker.findAndAndLinkNextUnlinkedEvent()) {
            if (stopWatch.getTime() > maxRuntimeMilliseconds) {
                break;
            }
        }
    }
}
