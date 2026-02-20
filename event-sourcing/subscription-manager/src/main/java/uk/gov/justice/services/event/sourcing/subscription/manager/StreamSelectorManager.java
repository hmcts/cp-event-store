package uk.gov.justice.services.event.sourcing.subscription.manager;

import static java.lang.String.format;

import uk.gov.justice.services.event.buffer.core.repository.subscription.LockedStreamStatus;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamProcessingException;

import java.util.Optional;

import javax.inject.Inject;
import javax.transaction.UserTransaction;

public class StreamSelectorManager {

    @Inject
    private StreamSelector streamSelector;

    @Inject
    private TransactionHandler transactionHandler;

    @Inject
    private UserTransaction userTransaction;

    public Optional<LockedStreamStatus> selectStreamToProcess(
            final String source,
            final String component) {
        try {
            return streamSelector.findStreamToProcess(source, component);
        } catch (final Exception e) {
            transactionHandler.rollback(userTransaction);
            throw new StreamProcessingException(format("Failed to find stream to process, source: '%s', component: '%s'", source, component), e);
        }
    }
}
