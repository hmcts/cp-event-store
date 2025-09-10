package uk.gov.justice.services.event.buffer.core.repository.subscription;

import static java.util.Optional.empty;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;
import uk.gov.justice.services.test.utils.persistence.TestJdbcDataSourceProvider;

import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class NewStreamStatusRepositoryIT {

    private static final String FRAMEWORK = "framework";

    @Mock
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    @Spy
    private NewStreamStatusRowMapper streamStatusRowMapper;

    @InjectMocks
    private NewStreamStatusRepository newStreamStatusRepository;

    @BeforeEach
    public void cleanDatabase() {
        new DatabaseCleaner().cleanStreamStatusTable(FRAMEWORK);
    }

    @Test
    public void shouldInsertRowInStreamStatusTableIfNoneExists() throws Exception {

        final DataSource viewStoreDataSource = new TestJdbcDataSourceProvider().getViewStoreDataSource(FRAMEWORK);
        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);

        final UUID streamId = randomUUID();

        final String source = "some-source";
        final String componentName = "some-component-name";
        final boolean upToDate = false;
        final ZonedDateTime updatedAt = new UtcClock().now().minusDays(2);

        assertThat(newStreamStatusRepository.findAll().isEmpty(), is(true));

        assertThat(newStreamStatusRepository.insertIfNotExists(
                streamId,
                source,
                componentName,
                updatedAt,
                upToDate), is(1));

        assertThat(newStreamStatusRepository.findAll().size(), is(1));

        final Optional<StreamStatus> streamStatus = newStreamStatusRepository.find(streamId, source, componentName);

        assertThat(streamStatus.isPresent(), is(true));
        assertThat(streamStatus.get().streamId(), is(streamId));
        assertThat(streamStatus.get().position(), is(0L));
        assertThat(streamStatus.get().source(), is(source));
        assertThat(streamStatus.get().component(), is(componentName));
        assertThat(streamStatus.get().streamErrorId(), is(empty()));
        assertThat(streamStatus.get().updatedAt(), is(updatedAt));
        assertThat(streamStatus.get().latestKnownPosition(), is(0L));
        assertThat(streamStatus.get().isUpToDate(), is(upToDate));

        final ZonedDateTime newUpdatedAt = new UtcClock().now();
        final boolean newIsUpToDate = true;

        final int rowsUpdated = newStreamStatusRepository.insertIfNotExists(
                streamId,
                source,
                componentName,
                newUpdatedAt,
                newIsUpToDate
        );

        assertThat(rowsUpdated, is(0));

        assertThat(newStreamStatusRepository.findAll().size(), is(1));

        final Optional<StreamStatus> idempotentStreamStatus = newStreamStatusRepository.find(streamId, source, componentName);

        assertThat(idempotentStreamStatus.isPresent(), is(true));
        assertThat(idempotentStreamStatus.get().streamId(), is(streamId));
        assertThat(idempotentStreamStatus.get().position(), is(0L));
        assertThat(idempotentStreamStatus.get().source(), is(source));
        assertThat(idempotentStreamStatus.get().component(), is(componentName));
        assertThat(idempotentStreamStatus.get().streamErrorId(), is(empty()));
        assertThat(idempotentStreamStatus.get().updatedAt(), is(updatedAt));
        assertThat(idempotentStreamStatus.get().latestKnownPosition(), is(0L));
        assertThat(idempotentStreamStatus.get().isUpToDate(), is(upToDate));
    }

    @Test
    public void shouldGetPositionInStreamAndLockRow() throws Exception {

        final DataSource viewStoreDataSource = new TestJdbcDataSourceProvider().getViewStoreDataSource(FRAMEWORK);
        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);

        final UUID streamId = randomUUID();

        final String source = "some-source";
        final String componentName = "some-component-name";
        final boolean upToDate = false;
        final long incomingEventPosition = 23L;
        final ZonedDateTime updatedAt = new UtcClock().now().minusDays(2);

        final int rowsUpdated = newStreamStatusRepository.insertIfNotExists(
                streamId,
                source,
                componentName,
                updatedAt,
                upToDate
        );

        assertThat(rowsUpdated, is(1));
        assertThat(newStreamStatusRepository.findAll().size(), is(1));

        final StreamUpdateContext streamUpdateContext = newStreamStatusRepository.lockStreamAndGetStreamUpdateContext(
                streamId,
                source,
                componentName,
                incomingEventPosition);
        assertThat(streamUpdateContext.currentStreamPosition(), is(0L));
        assertThat(streamUpdateContext.latestKnownStreamPosition(), is(0L));
        assertThat(streamUpdateContext.incomingEventPosition(), is(incomingEventPosition));
        assertThat(streamUpdateContext.streamErrorId(), is(empty()));
    }

    @Test
    public void shouldUpdatePositionOfAStream() throws Exception {

        final DataSource viewStoreDataSource = new TestJdbcDataSourceProvider().getViewStoreDataSource(FRAMEWORK);
        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);

        final UUID streamId = randomUUID();

        final String source = "some-source";
        final String componentName = "some-component-name";
        final boolean upToDate = false;
        final ZonedDateTime updatedAt = new UtcClock().now();
        final long newPosition = 23L;

        assertThat(newStreamStatusRepository.findAll().isEmpty(), is(true));

        assertThat(newStreamStatusRepository.insertIfNotExists(
                streamId,
                source,
                componentName,
                updatedAt,
                upToDate), is(1));

        final Optional<StreamStatus> streamStatus = newStreamStatusRepository.find(streamId, source, componentName);
        assertThat(streamStatus.isPresent(), is(true));
        assertThat(streamStatus.get().position(), is(0L));

        newStreamStatusRepository.updateCurrentPosition(
                streamId,
                source,
                componentName,
                newPosition
        );

        final Optional<StreamStatus> updatedStreamStatus = newStreamStatusRepository.find(streamId, source, componentName);
        assertThat(updatedStreamStatus.isPresent(), is(true));
        assertThat(updatedStreamStatus.get().position(), is(newPosition));
    }

    @Test
    public void shouldUpdateLatestPositionAndIsUpToDateOfAStream() throws Exception {

        final DataSource viewStoreDataSource = new TestJdbcDataSourceProvider().getViewStoreDataSource(FRAMEWORK);
        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);

        final UUID streamId = randomUUID();

        final String source = "some-source";
        final String componentName = "some-component-name";
        final boolean upToDate = false;
        final ZonedDateTime updatedAt = new UtcClock().now();
        final long latestKnownPosition = 23L;

        assertThat(newStreamStatusRepository.findAll().isEmpty(), is(true));

        assertThat(newStreamStatusRepository.insertIfNotExists(
                streamId,
                source,
                componentName,
                updatedAt,
                upToDate), is(1));
        newStreamStatusRepository.setUpToDate(true, streamId, source, componentName);

        final Optional<StreamStatus> streamStatus = newStreamStatusRepository.find(streamId, source, componentName);
        assertThat(streamStatus.isPresent(), is(true));
        assertThat(streamStatus.get().latestKnownPosition(), is(0L));
        assertThat(streamStatus.get().isUpToDate(), is(true));

        newStreamStatusRepository.updateLatestKnownPositionAndIsUpToDateToFalse(
                streamId,
                source,
                componentName,
                latestKnownPosition
        );

        final Optional<StreamStatus> updatedStreamStatus = newStreamStatusRepository.find(streamId, source, componentName);
        assertThat(updatedStreamStatus.isPresent(), is(true));
        assertThat(updatedStreamStatus.get().latestKnownPosition(), is(latestKnownPosition));
        assertThat(updatedStreamStatus.get().isUpToDate(), is(false));
    }

    @Test
    public void shouldSetStreamAsUpToDate() throws Exception {

        final DataSource viewStoreDataSource = new TestJdbcDataSourceProvider().getViewStoreDataSource(FRAMEWORK);
        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);

        final UUID streamId = randomUUID();

        final String source = "some-source";
        final String componentName = "some-component-name";
        final boolean upToDate = false;
        final ZonedDateTime updatedAt = new UtcClock().now();

        assertThat(newStreamStatusRepository.findAll().isEmpty(), is(true));

        assertThat(newStreamStatusRepository.insertIfNotExists(
                streamId,
                source,
                componentName,
                updatedAt,
                upToDate), is(1));

        final Optional<StreamStatus> streamStatus = newStreamStatusRepository.find(streamId, source, componentName);
        assertThat(streamStatus.isPresent(), is(true));
        assertThat(streamStatus.get().isUpToDate(), is(false));

        newStreamStatusRepository.setUpToDate(true, streamId, source, componentName);

        final Optional<StreamStatus> updatedStreamStatus = newStreamStatusRepository.find(streamId, source, componentName);
        assertThat(updatedStreamStatus.isPresent(), is(true));
        assertThat(updatedStreamStatus.get().isUpToDate(), is(true));
    }
}
