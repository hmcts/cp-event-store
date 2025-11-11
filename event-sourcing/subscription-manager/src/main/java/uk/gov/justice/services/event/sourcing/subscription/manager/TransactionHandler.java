package uk.gov.justice.services.event.sourcing.subscription.manager;

import static javax.transaction.Status.STATUS_MARKED_ROLLBACK;
import static javax.transaction.Status.STATUS_NO_TRANSACTION;
import static javax.transaction.Status.STATUS_ROLLING_BACK;

import uk.gov.justice.services.event.buffer.core.repository.subscription.TransactionException;

import javax.inject.Inject;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.UserTransaction;

import org.slf4j.Logger;

public class TransactionHandler {

    @Inject
    private Logger logger;

    public void begin(final UserTransaction userTransaction) {
        try {
            userTransaction.begin();
        } catch (final SystemException | NotSupportedException e) {
            throw new TransactionException("Failed to begin UserTransaction", e);
        }
    }

    public void commit(final UserTransaction userTransaction) {
        try {
            userTransaction.commit();
        } catch (final SystemException | RollbackException | HeuristicMixedException | HeuristicRollbackException e) {
            throw new TransactionException("Failed to commit UserTransaction", e);
        }
    }

    public void rollback(final UserTransaction userTransaction) {
        try {
            if (canRollBack(userTransaction)) {
                userTransaction.rollback();
            }
        } catch (final SystemException e) {
            logger.error("Failed to rollback transaction, rollback maybe incomplete", e);
        }
    }

    private boolean canRollBack(final UserTransaction userTransaction) throws SystemException {

        final int transactionStatus = userTransaction.getStatus();

        return transactionStatus != STATUS_NO_TRANSACTION &&
               transactionStatus != STATUS_MARKED_ROLLBACK &&
               transactionStatus != STATUS_ROLLING_BACK;
    }
}
