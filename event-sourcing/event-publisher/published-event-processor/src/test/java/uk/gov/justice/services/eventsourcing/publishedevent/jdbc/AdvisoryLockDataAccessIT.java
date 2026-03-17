package uk.gov.justice.services.eventsourcing.publishedevent.jdbc;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import uk.gov.justice.services.test.utils.persistence.FrameworkTestDataSourceFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class AdvisoryLockDataAccessIT {

    @InjectMocks
    private AdvisoryLockDataAccess advisoryLockDataAccess;

    @Test
    public void shouldTryToObtainNonBlockingTransactionalLockUsingProvidedConnection() throws Exception {
        final javax.sql.DataSource eventStoreDataSource = new FrameworkTestDataSourceFactory().createEventStoreDataSource();

        try (final java.sql.Connection connection = eventStoreDataSource.getConnection()) {
            assertThat(advisoryLockDataAccess.tryNonBlockingTransactionLevelAdvisoryLock(connection, 42L), is(true));
        }
    }
}