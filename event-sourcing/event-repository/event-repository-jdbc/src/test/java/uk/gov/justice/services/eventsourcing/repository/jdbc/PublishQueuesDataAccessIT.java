package uk.gov.justice.services.eventsourcing.repository.jdbc;

import static java.time.ZoneOffset.UTC;
import static java.time.ZonedDateTime.of;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.eventsourcing.repository.jdbc.PublishQueue.PublishQueueTableName.PUBLISH_QUEUE_TABLE;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;
import uk.gov.justice.services.test.utils.core.eventsource.EventStoreInitializer;
import uk.gov.justice.services.test.utils.persistence.FrameworkTestDataSourceFactory;

import java.time.ZonedDateTime;
import java.util.UUID;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class PublishQueuesDataAccessIT {

    private final DataSource eventStoreDataSource = new FrameworkTestDataSourceFactory().createEventStoreDataSource();

    @Mock
    private EventStoreDataSourceProvider eventStoreDataSourceProvider;

    @InjectMocks
    private PublishQueuesDataAccess publishQueuesDataAccess;

    @BeforeEach
    public void initDatabase() throws Exception {
        new EventStoreInitializer().initializeEventStore(eventStoreDataSource);
    }

    @Test
    public void shouldPopEventsFromThePublishQueue() throws Exception {

        final UUID eventId_1 = randomUUID();
        final UUID eventId_2 = randomUUID();
        final UUID eventId_3 = randomUUID();

        final ZonedDateTime queuedAt_1 = of(2021, 8, 23, 11, 11, 1, 0, UTC);
        final ZonedDateTime queuedAt_2 = of(2021, 8, 23, 11, 11, 2, 0, UTC);
        final ZonedDateTime queuedAt_3 = of(2021, 8, 23, 11, 11, 3, 0, UTC);

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(eventStoreDataSource);

        assertThat(publishQueuesDataAccess.popNextEventId(PUBLISH_QUEUE_TABLE).isPresent(), is(false));

        publishQueuesDataAccess.addToQueue(eventId_1, queuedAt_1, PUBLISH_QUEUE_TABLE);
        publishQueuesDataAccess.addToQueue(eventId_2, queuedAt_2, PUBLISH_QUEUE_TABLE);
        publishQueuesDataAccess.addToQueue(eventId_3, queuedAt_3, PUBLISH_QUEUE_TABLE);

        assertThat(publishQueuesDataAccess.getSizeOfQueue(PUBLISH_QUEUE_TABLE), is(3));
        assertThat(publishQueuesDataAccess.popNextEventId(PUBLISH_QUEUE_TABLE).get(), is(eventId_1));
        assertThat(publishQueuesDataAccess.getSizeOfQueue(PUBLISH_QUEUE_TABLE), is(2));
        assertThat(publishQueuesDataAccess.popNextEventId(PUBLISH_QUEUE_TABLE).get(), is(eventId_2));
        assertThat(publishQueuesDataAccess.getSizeOfQueue(PUBLISH_QUEUE_TABLE), is(1));
        assertThat(publishQueuesDataAccess.popNextEventId(PUBLISH_QUEUE_TABLE).get(), is(eventId_3));
        assertThat(publishQueuesDataAccess.getSizeOfQueue(PUBLISH_QUEUE_TABLE), is(0));

        assertThat(publishQueuesDataAccess.popNextEventId(PUBLISH_QUEUE_TABLE).isPresent(), is(false));

    }

    @Test
    public void shouldGetTheSizeOfTheQueue() throws Exception {

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(eventStoreDataSource);

        publishQueuesDataAccess.addToQueue(randomUUID(), new UtcClock().now(), PUBLISH_QUEUE_TABLE);
        publishQueuesDataAccess.addToQueue(randomUUID(), new UtcClock().now(), PUBLISH_QUEUE_TABLE);
        publishQueuesDataAccess.addToQueue(randomUUID(), new UtcClock().now(), PUBLISH_QUEUE_TABLE);

        assertThat(publishQueuesDataAccess.getSizeOfQueue(PUBLISH_QUEUE_TABLE), is(3));
    }
}
