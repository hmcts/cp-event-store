package uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.util;

import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.manager.ConcurrentEventStreamConsumerManager;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.PublishedEvent;
import uk.gov.justice.services.eventstore.management.commands.EventCatchupCommand;

import javax.ejb.Singleton;
import javax.inject.Inject;
import java.util.UUID;
import java.util.stream.Stream;

import static java.util.UUID.randomUUID;

@Singleton
public class TestCatchupBean {

    @Inject
    private DummyCatchupEventProcessor transactionalEventProcessor;

    @Inject
    private ConcurrentEventStreamConsumerManager concurrentEventStreamConsumerManager;

    @Inject
    Logger logger;

    public void run(final StopWatch stopWatch) {
        logger.info("running!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

        final int numberOfStreams = 10;
        final int numberOfEventsToCreate = 60;
        final int numberOfUniqueEventNames = 10;

        final UUID commandId = randomUUID();
        final EventCatchupCommand eventCatchupCommand = new EventCatchupCommand();

        transactionalEventProcessor.setExpectedNumberOfEvents(numberOfEventsToCreate);

        final EventFactory eventFactory = new EventFactory(numberOfStreams, numberOfUniqueEventNames);
        final Stream<PublishedEvent> eventStream = eventFactory.generateEvents(numberOfEventsToCreate).stream();


        stopWatch.start();
        final int totalEventsProcessed = eventStream.mapToInt(event -> {
            concurrentEventStreamConsumerManager.add(event, "subscriptionName", eventCatchupCommand, commandId);
            return 1;
        }).sum();

        concurrentEventStreamConsumerManager.waitForCompletion();

        logger.info("Total events processed: " + totalEventsProcessed);
    }
}
