package uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.util;

import uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.manager.CatchupEventProcessor;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.PublishedEvent;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.openejb.testing.Default;

@Default
public class DummyCatchupEventProcessor implements CatchupEventProcessor {

    private static final int SLEEP_TIME = 10;

    private final Queue<PublishedEvent> publishedEvents = new ConcurrentLinkedQueue<>();
    private int expectedNumberOfEvents = 0;

    public void setExpectedNumberOfEvents(final int expectedNumberOfEvents) {
        this.expectedNumberOfEvents = expectedNumberOfEvents;
    }

    @Override
    public int processWithEventBuffer(final PublishedEvent event, final String subscriptionName) {

        publishedEvents.add(event);

        try {
            Thread.sleep(SLEEP_TIME);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return 1;
    }

    public Queue<PublishedEvent> getPublishedEvents() {
        return publishedEvents;
    }

    public boolean isComplete() {
        return publishedEvents.size() >= expectedNumberOfEvents;
    }
}
