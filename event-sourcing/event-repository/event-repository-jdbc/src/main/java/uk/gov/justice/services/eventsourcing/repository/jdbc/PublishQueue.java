package uk.gov.justice.services.eventsourcing.repository.jdbc;

import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;

public interface PublishQueue {
    enum PublishQueueTableName {

        PUBLISH_QUEUE_TABLE("publish_queue");

        private final String tableName;

        PublishQueueTableName(final String tableName) {
            this.tableName = tableName;
        }

        public String getTableName() {
            return tableName;
        }
    }

    void addToQueue(final UUID eventId, final ZonedDateTime queuedAt);
    Optional<UUID> popNextEventId();
    int getSizeOfQueue();
}
