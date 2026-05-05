package uk.gov.justice.services.eventsourcing.repository.jdbc;

import static java.util.UUID.fromString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.eventsourcing.repository.jdbc.PublishQueue.PublishQueueTableName.PUBLISH_QUEUE_TABLE;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.repository.jdbc.exception.PublishedEventException;
import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;

import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.UUID;

import jakarta.inject.Inject;
import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class PublishQueuesDataAccessTest {

    @Mock
    @Inject
    private EventStoreDataSourceProvider eventStoreDataSourceProvider;

    @InjectMocks
    private PublishQueuesDataAccess publishQueuesDataAccess;

    @Test
    public void shouldRethrowSqlExceptionIfAddToQueueFails() throws Exception {

        final UUID eventId = fromString("fc9ac4d0-66ba-4b78-a114-875c6950c64d");
        final ZonedDateTime queuedAt = new UtcClock().now();
        final SQLException sqlException = new SQLException("Oops");

        final DataSource dataSource = mock(DataSource.class);

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenThrow(sqlException);

        final PublishedEventException publishedEventException = assertThrows(PublishedEventException.class, () -> publishQueuesDataAccess.addToQueue(
                eventId,
                queuedAt,
                PUBLISH_QUEUE_TABLE));

        assertThat(publishedEventException.getMessage(), is("Failed to add eventId 'fc9ac4d0-66ba-4b78-a114-875c6950c64d' to publish_queue table"));
        assertThat(publishedEventException.getCause(), is(sqlException));
    }

    @Test
    public void shouldRethrowSqlExceptionIfPopNextEventIdFails() throws Exception {

        final SQLException sqlException = new SQLException("Oops");

        final DataSource dataSource = mock(DataSource.class);

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenThrow(sqlException);

        final PublishedEventException publishedEventException = assertThrows(PublishedEventException.class, () -> publishQueuesDataAccess.popNextEventId(PUBLISH_QUEUE_TABLE));

        assertThat(publishedEventException.getMessage(), is("Failed to read event_id from publish_queue table"));
        assertThat(publishedEventException.getCause(), is(sqlException));
    }

    @Test
    public void shouldRethrowSqlExceptionIfGetSizeOfQueueFails() throws Exception {

        final SQLException sqlException = new SQLException("Oops");

        final DataSource dataSource = mock(DataSource.class);

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenThrow(sqlException);

        final PublishedEventException publishedEventException = assertThrows(PublishedEventException.class, () -> publishQueuesDataAccess.getSizeOfQueue(PUBLISH_QUEUE_TABLE));

        assertThat(publishedEventException.getMessage(), is("Failed to count rows of publish_queue table"));
        assertThat(publishedEventException.getCause(), is(sqlException));
    }
}