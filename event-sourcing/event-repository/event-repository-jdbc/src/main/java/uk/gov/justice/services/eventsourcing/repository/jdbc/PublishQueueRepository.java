package uk.gov.justice.services.eventsourcing.repository.jdbc;

import static uk.gov.justice.services.eventsourcing.repository.jdbc.PublishQueue.PublishQueueTableName.PUBLISH_QUEUE_TABLE;

import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;

import jakarta.inject.Inject;

public class PublishQueueRepository implements PublishQueue {

    @Inject
    private PublishQueuesDataAccess publishQueuesDataAccess;

    @Override
    public void addToQueue(final UUID eventId, final ZonedDateTime queuedAt) {
        publishQueuesDataAccess.addToQueue(eventId, queuedAt, PUBLISH_QUEUE_TABLE);
    }

    @Override
    public Optional<UUID> popNextEventId() {
        return publishQueuesDataAccess.popNextEventId(PUBLISH_QUEUE_TABLE);
    }

    @Override
    public int getSizeOfQueue() {
        return publishQueuesDataAccess.getSizeOfQueue(PUBLISH_QUEUE_TABLE);
    }
}
