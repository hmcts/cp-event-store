package uk.gov.justice.services.healthcheck.healthchecks.artemis;

import uk.gov.justice.services.messaging.jms.DestinationProvider;

import java.util.ArrayList;
import java.util.List;

import jakarta.inject.Inject;
import jakarta.jms.JMSException;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;

import org.slf4j.Logger;

public class JmsDestinationsVerifier {

    @Inject
    private Logger logger;

    @Inject
    private DestinationNamesProvider destinationNamesProvider;

    @Inject
    private DestinationProvider destinationProvider;

    public void verify(final Session session) throws DestinationNotFoundException {
        final var destinationNames = destinationNamesProvider.getDestinationNames();

        final List<String> failedDestinationNames = new ArrayList<>();
        for (final String destinationName : destinationNames) {
            try {
                verifyThatDestinationExist(session, destinationName);
            } catch (final Exception e) {
                this.logger.error("Error verifying existence of destination: " + destinationName, e);
                failedDestinationNames.add(destinationName);
            }
        }

        if(failedDestinationNames.size() != 0) {
            final String failedDestinationNamesFormatted = String.join(", ", failedDestinationNames);
            throw new DestinationNotFoundException(String.format("Destination(s) %s not exist", failedDestinationNamesFormatted));
        }
    }

    private void verifyThatDestinationExist(final Session session, final String destinationName) throws JMSException {
        final var destination = destinationProvider.getDestination(destinationName);

        try (final MessageConsumer consumer = session.createConsumer(destination)) {
        }
    }
}
