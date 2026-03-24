package uk.gov.justice.services.eventsourcing.publishedevent.jdbc;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import uk.gov.justice.services.test.utils.persistence.FrameworkTestDataSourceFactory;

import java.sql.Connection;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class AdvisoryLockDataAccessIT {

    @InjectMocks
    private AdvisoryLockDataAccess advisoryLockDataAccess;

    @Test
    public void shouldObtainBlockingTransactionalLock() throws Exception {

        final DataSource eventStoreDataSource = new FrameworkTestDataSourceFactory().createEventStoreDataSource();

        try (final Connection connection = eventStoreDataSource.getConnection()) {
            advisoryLockDataAccess.obtainBlockingTransactionLevelAdvisoryLock(connection, 23L);
        }
    }

    @Test
    public void shouldTryToObtainNonBlockingTransactionalLock() throws Exception {
        final DataSource eventStoreDataSource = new FrameworkTestDataSourceFactory().createEventStoreDataSource();

        try (final Connection connection = eventStoreDataSource.getConnection()) {
            assertThat(advisoryLockDataAccess.tryNonBlockingTransactionLevelAdvisoryLock(connection, 23L), is(true));
        }
    }
}