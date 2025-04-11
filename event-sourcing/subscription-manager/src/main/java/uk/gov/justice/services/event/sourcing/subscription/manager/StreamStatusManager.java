package uk.gov.justice.services.event.sourcing.subscription.manager;

import static java.lang.String.format;
import static javax.transaction.Status.STATUS_NO_TRANSACTION;
import static javax.transaction.Transactional.TxType.REQUIRES_NEW;

import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRepository;
import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamStatusException;
import uk.gov.justice.services.event.buffer.core.repository.subscription.TransactionException;

import java.time.ZonedDateTime;
import java.util.UUID;

import javax.inject.Inject;
import javax.transaction.SystemException;
import javax.transaction.Transactional;
import javax.transaction.UserTransaction;

public class StreamStatusManager {

    @Inject
    private UserTransaction userTransaction;

    @Inject
    private NewStreamStatusRepository newStreamStatusRepository;

    @Transactional(REQUIRES_NEW)
    public void createNewStreamInStreamStatusTableIfNecessary(
            final UUID streamId,
            final String source,
            final String componentName,
            final ZonedDateTime updatedAt,
            final boolean isUpToDate) {
        try {
            userTransaction.begin();

            newStreamStatusRepository.insertIfNotExists(
                    streamId,
                    source,
                    componentName,
                    updatedAt,
                    isUpToDate);

            userTransaction.commit();
        } catch (final Exception e) {
            rollbackTransaction();
            throw new StreamStatusException(
                    format("Failed to insert stream into stream_status table: streamId '%s' source '%s' component '%s'",
                            streamId,
                            source,
                            componentName),
                    e);
        }
    }

    private void rollbackTransaction() {
        try {
            if (userTransaction.getStatus() != STATUS_NO_TRANSACTION) {
                userTransaction.rollback();
            }
        } catch(final SystemException e){
            throw new TransactionException("Unexpected exception during transaction rollback, rollback maybe incomplete", e);
        }
    }
}
