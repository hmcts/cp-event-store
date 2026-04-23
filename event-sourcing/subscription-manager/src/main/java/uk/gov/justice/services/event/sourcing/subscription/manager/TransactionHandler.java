package uk.gov.justice.services.event.sourcing.subscription.manager;

import static jakarta.transaction.Status.STATUS_NO_TRANSACTION;

import uk.gov.justice.services.event.buffer.core.repository.subscription.TransactionException;

import jakarta.inject.Inject;
import jakarta.transaction.HeuristicMixedException;
import jakarta.transaction.HeuristicRollbackException;
import jakarta.transaction.NotSupportedException;
import jakarta.transaction.RollbackException;
import jakarta.transaction.SystemException;
import jakarta.transaction.UserTransaction;

import org.slf4j.Logger;

public class TransactionHandler {

    @Inject
    private UserTransaction userTransaction;

    @Inject
    private Logger logger;

    public void begin() {
        try {
            userTransaction.begin();
        } catch (final SystemException | NotSupportedException e) {
            throw new TransactionException("Failed to begin UserTransaction", e);
        }
    }

    public void commit() {
        try {
            userTransaction.commit();
        } catch (final SystemException | RollbackException | HeuristicMixedException | HeuristicRollbackException e) {
            throw new TransactionException("Failed to commit UserTransaction", e);
        }
    }

    public void rollback() {
        try {

            if (userTransaction.getStatus() != STATUS_NO_TRANSACTION) {
                userTransaction.rollback();
            }
        } catch (final SystemException | IllegalStateException e) {
            logger.error("Failed to rollback transaction, rollback maybe incomplete", e);
        }
    }

}
