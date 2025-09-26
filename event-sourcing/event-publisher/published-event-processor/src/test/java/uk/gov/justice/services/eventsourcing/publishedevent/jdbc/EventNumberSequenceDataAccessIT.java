package uk.gov.justice.services.eventsourcing.publishedevent.jdbc;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;
import uk.gov.justice.services.test.utils.persistence.FrameworkTestDataSourceFactory;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EventNumberSequenceDataAccessIT {

    @Spy
    private EventStoreDataSourceProvider eventStoreDataSourceProvider;

    @InjectMocks
    private EventNumberSequenceDataAccess eventNumberSequenceDataAccess;

    @Test
    public void shouldLockReadAndUpdateNextEventNumberSequence() throws Exception {

        final Long nextEventNumber = 23L;
        final DataSource eventStoreDataSource = new FrameworkTestDataSourceFactory().createEventStoreDataSource();
        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(eventStoreDataSource);

        eventNumberSequenceDataAccess.updateNextAvailableEventNumberTo(nextEventNumber);
        assertThat(eventNumberSequenceDataAccess.lockAndGetNextAvailableEventNumber(), is(nextEventNumber));
        eventNumberSequenceDataAccess.updateNextAvailableEventNumberTo(nextEventNumber + 1);
        assertThat(eventNumberSequenceDataAccess.lockAndGetNextAvailableEventNumber(), is(nextEventNumber + 1));
    }
}