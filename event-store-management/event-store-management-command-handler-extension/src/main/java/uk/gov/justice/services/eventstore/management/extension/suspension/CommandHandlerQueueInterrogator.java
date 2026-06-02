package uk.gov.justice.services.eventstore.management.extension.suspension;

import uk.gov.justice.services.common.polling.MultiIteratingPoller;
import uk.gov.justice.services.common.polling.MultiIteratingPollerFactory;

import jakarta.inject.Inject;

public class CommandHandlerQueueInterrogator {

    @Inject
    private CommandHandlerQueueSupplierFactory commandHandlerQueueSupplierFactory;

    @Inject
    private MultiIteratingPollerFactory multiIteratingPollerFactory;

    public boolean pollUntilEmptyHandlerQueue() {

        final MultiIteratingPoller multiIteratingPoller = multiIteratingPollerFactory.create(5, 1000L, 3, 500L);

        return multiIteratingPoller.pollUntilTrue(commandHandlerQueueSupplierFactory.isCommandHandlerQueueEmpty());
    }
}
