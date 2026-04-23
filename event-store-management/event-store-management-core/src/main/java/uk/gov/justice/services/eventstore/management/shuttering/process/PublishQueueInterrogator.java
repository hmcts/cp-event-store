package uk.gov.justice.services.eventstore.management.shuttering.process;

import uk.gov.justice.services.common.polling.MultiIteratingPoller;
import uk.gov.justice.services.common.polling.MultiIteratingPollerFactory;
import uk.gov.justice.services.eventsourcing.repository.jdbc.PublishQueueRepository;

import jakarta.inject.Inject;

public class PublishQueueInterrogator {

    private static final int POLLER_RETRY_COUNT = 3;
    private static final long POLLER_DELAY_INTERVAL_MILLIS = 500L;
    private static final int NUMBER_OF_POLLING_ITERATIONS = 3;
    private static final long WAIT_TIME_BETWEEN_ITERATIONS_MILLIS = 500L;

    @Inject
    private PublishQueueRepository publishQueueRepository;

    @Inject
    private MultiIteratingPollerFactory multiIteratingPollerFactory;

    public boolean pollUntilPublishQueueEmpty() {

        final MultiIteratingPoller multiIteratingPoller = multiIteratingPollerFactory.create(
                POLLER_RETRY_COUNT,
                POLLER_DELAY_INTERVAL_MILLIS,
                NUMBER_OF_POLLING_ITERATIONS,
                WAIT_TIME_BETWEEN_ITERATIONS_MILLIS);

        return multiIteratingPoller.pollUntilTrue(() -> publishQueueRepository.getSizeOfQueue() == 0);
    }
}
