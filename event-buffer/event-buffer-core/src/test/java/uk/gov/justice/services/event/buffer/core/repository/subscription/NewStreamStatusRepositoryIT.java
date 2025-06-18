package uk.gov.justice.services.event.buffer.core.repository.subscription;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorDetails;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorDetailsPersistence;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorHash;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorHashPersistence;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;
import uk.gov.justice.services.test.utils.persistence.TestJdbcDataSourceProvider;

import static java.util.Optional.empty;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class NewStreamStatusRepositoryIT {

    private static final String FRAMEWORK = "framework";

    @Mock
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    @InjectMocks
    private NewStreamStatusRepository newStreamStatusRepository;

    @InjectMocks
    private StreamErrorHashPersistence streamErrorHashPersistence;

    @InjectMocks
    private StreamErrorDetailsPersistence streamErrorDetailsPersistence;

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

    @Nested
    class FindByErrorHashTest {

        private static final String SOURCE = "some-source";
        private static final String COMPONENT_NAME = "some-component-name";

        @Test
        public void shouldQueryAllStreamsByErrorHash() throws Exception {
            final DataSource viewStoreDataSource = new TestJdbcDataSourceProvider().getViewStoreDataSource(FRAMEWORK);
            when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);

            final UUID stream1Id = randomUUID();
            final UUID stream2Id = randomUUID();
            final boolean upToDate = false;
            final ZonedDateTime updatedAt = new UtcClock().now().minusDays(2);
            final String error1Hash = "hash-1";
            final String error2Hash = "hash-2";

            assertThat(newStreamStatusRepository.findBy(error1Hash).isEmpty(), is(true));

            newStreamStatusRepository.insertIfNotExists(stream1Id, SOURCE, COMPONENT_NAME, updatedAt, upToDate);
            newStreamStatusRepository.insertIfNotExists(stream2Id, SOURCE, COMPONENT_NAME, updatedAt, upToDate);
            insertEntriesToStreamErrorHash(error1Hash, error2Hash, viewStoreDataSource);
            insertEntryToStreamError(stream1Id, error1Hash, 1L, viewStoreDataSource);
            insertEntryToStreamError(stream1Id, error1Hash, 2L, viewStoreDataSource);
            insertEntryToStreamError(stream2Id, error2Hash, 1L, viewStoreDataSource);

            final List<StreamStatus> streamStatuses = newStreamStatusRepository.findBy(error1Hash);

            assertThat(streamStatuses.size(), is(1));
            final StreamStatus streamStatus = streamStatuses.get(0);
            assertThat(streamStatus.streamId(), is(stream1Id));
            assertThat(streamStatus.latestKnownPosition(), is(0L));
            assertThat(streamStatus.position(), is(0L));
            assertThat(streamStatus.updatedAt(), is(updatedAt));
            assertThat(streamStatus.component(), is(COMPONENT_NAME));
            assertThat(streamStatus.source(), is(SOURCE));
        }

        private void insertEntriesToStreamErrorHash(String error1Hash, String error2Hash, DataSource dataSource) throws Exception {
            final StreamErrorHash streamError1Hash = new StreamErrorHash(error1Hash.toString(), "java.lang.NullPointerException", Optional.empty(), "java.lang.NullPointerException", "find", 1);
            final StreamErrorHash streamError2Hash = new StreamErrorHash(error2Hash.toString(), "java.lang.IllegalArgumentException", Optional.empty(), "java.lang.IllegalArgumentException", "find1", 2);
            streamErrorHashPersistence.upsert(streamError1Hash, dataSource.getConnection());
            streamErrorHashPersistence.upsert(streamError2Hash, dataSource.getConnection());
        }

        private void insertEntryToStreamError(final UUID stream1Id, final String error1Hash,
                                              final Long position, final DataSource dataSource) throws Exception {
            final StreamErrorDetails streamErrorDetails = new StreamErrorDetails(
                    randomUUID(), error1Hash.toString(), "some-exception-message", empty(),
                    "event-name", randomUUID(), stream1Id, position,
                    new UtcClock().now(), "stack-trace",
                    COMPONENT_NAME, SOURCE
            );
            streamErrorDetailsPersistence.insert(streamErrorDetails, dataSource.getConnection());
        }
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

        final StreamPositions streamPositions = newStreamStatusRepository.lockRowAndGetPositions(
                streamId,
                source,
                componentName,
                incomingEventPosition);
        assertThat(streamPositions.currentStreamPosition(), is(0L));
        assertThat(streamPositions.latestKnownStreamPosition(), is(0L));
        assertThat(streamPositions.incomingEventPosition(), is(incomingEventPosition));
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
