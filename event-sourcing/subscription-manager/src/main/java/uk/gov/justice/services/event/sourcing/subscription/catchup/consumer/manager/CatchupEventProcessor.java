package uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.manager;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.PublishedEvent;

public interface CatchupEventProcessor {

    int processWithEventBuffer(final PublishedEvent event, final String subscriptionName);
}
