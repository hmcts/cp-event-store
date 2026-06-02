package uk.gov.justice.services.eventstore.management.aggregate.snapshot.regeneration.commands;

import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.util.Objects.requireNonNullElse;

import uk.gov.justice.services.common.configuration.Value;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class AggregateSnapshotGenerationCommandConfiguration {

    private static final String TWENTY_HOURS_IN_SECONDS = "" + 20 * 60 * 60;
    private static final String JNDI_VALUE_NAME = "jmx.aggregate.snapshot.generation.timout.seconds";

    @Inject
    @Value(key = JNDI_VALUE_NAME, defaultValue = TWENTY_HOURS_IN_SECONDS)
    private String transactionTimoutSeconds;

    public int getTransactionTimoutSeconds() {
        try {
            return parseInt(requireNonNullElse(transactionTimoutSeconds, TWENTY_HOURS_IN_SECONDS));
        } catch (final NumberFormatException e) {
            throw new AggregateSnapshotGenerationFailedException(format("Failed to parse jndi value '%s'. Value '%s' is not a number", JNDI_VALUE_NAME, transactionTimoutSeconds), e);
        }
    }
}
