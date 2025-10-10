package uk.gov.justice.services.eventsourcing.publishedevent.jdbc;

import static java.lang.String.format;
import static javax.transaction.Transactional.TxType.MANDATORY;

import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.inject.Inject;
import javax.transaction.Transactional;

public class AdvisoryLockDataAccess {

    static final String BLOCKING_TRANSACTION_LEVEL_ADVISORY_LOCK_SQL = """
            SELECT pg_advisory_xact_lock(?)
            """;
    static final String NON_BLOCKING_TRANSACTION_LEVEL_ADVISORY_LOCK_SQL = """
            SELECT pg_try_advisory_xact_lock(?);
            """;

    @Inject
    private EventStoreDataSourceProvider eventStoreDataSourceProvider;

    /**
     * Gets a transaction level advisory lock in postgres.
     * This method will block until the advisory lock becomes available.
     * Lock will release on transaction commit
     */
    @Transactional(MANDATORY)
    public void obtainBlockingTransactionLevelAdvisoryLock(final Long advisoryLockId) {

        try (final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
             final PreparedStatement lockPreparedStatement = connection.prepareStatement(BLOCKING_TRANSACTION_LEVEL_ADVISORY_LOCK_SQL)) {
            lockPreparedStatement.setLong(1, advisoryLockId);
            lockPreparedStatement.execute();
        } catch (final SQLException e) {
            throw new AdvisoryLockException(format("Failed to obtain blocking advisory lock on postgres with lock key '%d'", advisoryLockId), e);
        }
    }

    /**
     * Attempts to create a transaction level advisory lock in postgres.
     * This method will not block and will return true if the lock is granted, false if lock currently in use
     * Lock will release on transaction commit
     *
     * @return true if lock granted, false if not
     */
    @Transactional(MANDATORY)
    public boolean tryNonBlockingTransactionLevelAdvisoryLock(final Long advisoryLockId) {

        try (final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
             final PreparedStatement lockPreparedStatement = connection.prepareStatement(NON_BLOCKING_TRANSACTION_LEVEL_ADVISORY_LOCK_SQL)) {
            lockPreparedStatement.setLong(1, advisoryLockId);

            try(final ResultSet resultSet = lockPreparedStatement.executeQuery()) {

                if(resultSet.next()) {
                    return resultSet.getBoolean(1);
                }

                throw new AdvisoryLockException(format("Unable to obtain non-blocking advisory lock for id '%d'. No result returned when obtaining lock", advisoryLockId));
            }
        } catch (final SQLException e) {
            throw new AdvisoryLockException(format("Failed to obtain non-blocking advisory lock on database with lock key '%d'", advisoryLockId), e);
        }
    }
}
