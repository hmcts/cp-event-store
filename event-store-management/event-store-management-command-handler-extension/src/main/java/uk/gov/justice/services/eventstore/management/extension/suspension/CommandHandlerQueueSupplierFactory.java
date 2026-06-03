package uk.gov.justice.services.eventstore.management.extension.suspension;

import uk.gov.justice.services.messaging.jms.JmsCommandHandlerDestinationNameProvider;
import uk.gov.justice.services.messaging.jms.JmsQueueBrowser;

import java.util.function.BooleanSupplier;

import jakarta.inject.Inject;

public class CommandHandlerQueueSupplierFactory {

    @Inject
    private JmsQueueBrowser jmsQueueBrowser;

    @Inject
    private JmsCommandHandlerDestinationNameProvider jmsCommandHandlerDestinationNameProvider;

    public BooleanSupplier isCommandHandlerQueueEmpty() {
        return () -> jmsQueueBrowser.sizeOf(jmsCommandHandlerDestinationNameProvider.destinationName()) < 1;
    }
}
