package uk.gov.justice.services.event.buffer.core.repository.subscription;

import java.sql.Connection;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamError;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorHash;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorHashPersistence;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorOccurrence;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorOccurrencePersistence;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorPersistence;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorRetry;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorRetryRepository;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamStatusErrorPersistence;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;
import uk.gov.justice.services.test.utils.persistence.TestJdbcDataSourceProvider;

import static java.util.Optional.empty;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.test.utils.core.reflection.ReflectionUtil.setField;

@ExtendWith(MockitoExtension.class)
public class NewStreamStatusRepositoryIT {

    private static final String FRAMEWORK = "framework";
    private static final Integer MAX_RETRIES = 10;

    @Mock
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    @Spy
    private NewStreamStatusRowMapper streamStatusRowMapper;

    @InjectMocks
    private NewStreamStatusRepository newStreamStatusRepository;

    @BeforeEach
    public void cleanDatabase() {
        final DatabaseCleaner databaseCleaner = new DatabaseCleaner();
        databaseCleaner.cleanViewStoreTables(FRAMEWORK, "stream_error_retry");
        databaseCleaner.cleanViewStoreErrorTables(FRAMEWORK);
        databaseCleaner.cleanStreamStatusTable(FRAMEWORK);
    }

    @Test
    public void shouldInsertRowInStreamStatusTableIfNoneExists() throws Exception {

        final DataSource viewStoreDataSource = new TestJdbcDataSourceProvider().getViewStoreDataSource(FRAMEWORK);
        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);

        final UUID streamId = randomUUID();

        final String source = "some-source";
        final String componentName = "some-component-name";
        final boolean upToDate = false;
        final ZonedDateTime discoveredAt = new UtcClock().now().minusDays(2);

        assertThat(newStreamStatusRepository.findAll().isEmpty(), is(true));

        assertThat(newStreamStatusRepository.insertIfNotExists(
                streamId,
                source,
                componentName,
                discoveredAt,
                upToDate), is(1));

        assertThat(newStreamStatusRepository.findAll().size(), is(1));

        final Optional<StreamStatus> streamStatus = newStreamStatusRepository.find(streamId, source, componentName);

        assertThat(streamStatus.isPresent(), is(true));
        assertThat(streamStatus.get().streamId(), is(streamId));
        assertThat(streamStatus.get().position(), is(0L));
        assertThat(streamStatus.get().source(), is(source));
        assertThat(streamStatus.get().component(), is(componentName));
        assertThat(streamStatus.get().streamErrorId(), is(empty()));
        assertThat(streamStatus.get().discoveredAt(), is(discoveredAt));
        assertThat(streamStatus.get().latestKnownPosition(), is(0L));
        assertThat(streamStatus.get().isUpToDate(), is(upToDate));

        final ZonedDateTime newDiscoveredAt = new UtcClock().now();
        final boolean newIsUpToDate = true;

        final int rowsUpdated = newStreamStatusRepository.insertIfNotExists(
                streamId,
                source,
                componentName,
                newDiscoveredAt,
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
        assertThat(idempotentStreamStatus.get().discoveredAt(), is(discoveredAt));
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
        final ZonedDateTime discoveredAt = new UtcClock().now().minusDays(2);

        final int rowsUpdated = newStreamStatusRepository.insertIfNotExists(
                streamId,
                source,
                componentName,
                discoveredAt,
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
        final ZonedDateTime discoveredAt = new UtcClock().now();
        final long newPosition = 23L;

        assertThat(newStreamStatusRepository.findAll().isEmpty(), is(true));

        assertThat(newStreamStatusRepository.insertIfNotExists(
                streamId,
                source,
                componentName,
                discoveredAt,
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
    public void shouldUpdateLatestPosition() throws Exception {

        final DataSource viewStoreDataSource = new TestJdbcDataSourceProvider().getViewStoreDataSource(FRAMEWORK);
        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);

        final UUID streamId = randomUUID();

        final String source = "some-source";
        final String componentName = "some-component-name";
        final boolean upToDate = true;
        final ZonedDateTime discoveredAt = new UtcClock().now();
        final long latestKnownPosition = 23L;

        assertThat(newStreamStatusRepository.findAll().isEmpty(), is(true));

        assertThat(newStreamStatusRepository.insertIfNotExists(
                streamId,
                source,
                componentName,
                discoveredAt,
                upToDate), is(1));

        final Optional<StreamStatus> streamStatus = newStreamStatusRepository.find(streamId, source, componentName);
        assertThat(streamStatus.isPresent(), is(true));
        assertThat(streamStatus.get().latestKnownPosition(), is(0L));
        assertThat(streamStatus.get().isUpToDate(), is(true));

        newStreamStatusRepository.upsertLatestKnownPosition(
                streamId,
                source,
                componentName,
                latestKnownPosition,
                discoveredAt
        );

        final Optional<StreamStatus> updatedStreamStatus = newStreamStatusRepository.find(streamId, source, componentName);
        assertThat(updatedStreamStatus.isPresent(), is(true));
        assertThat(updatedStreamStatus.get().latestKnownPosition(), is(latestKnownPosition));
        assertThat(updatedStreamStatus.get().isUpToDate(), is(false));
    }

    @Test
    public void shouldUpdateLatestPositionAndIsUpToDateOfAStream() throws Exception {

        final DataSource viewStoreDataSource = new TestJdbcDataSourceProvider().getViewStoreDataSource(FRAMEWORK);
        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);

        final UUID streamId = randomUUID();

        final String source = "some-source";
        final String componentName = "some-component-name";
        final boolean upToDate = false;
        final ZonedDateTime discoveredAt = new UtcClock().now();
        final long latestKnownPosition = 23L;

        assertThat(newStreamStatusRepository.findAll().isEmpty(), is(true));

        assertThat(newStreamStatusRepository.insertIfNotExists(
                streamId,
                source,
                componentName,
                discoveredAt,
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
        final ZonedDateTime discoveredAt = new UtcClock().now();

        assertThat(newStreamStatusRepository.findAll().isEmpty(), is(true));

        assertThat(newStreamStatusRepository.insertIfNotExists(
                streamId,
                source,
                componentName,
                discoveredAt,
                upToDate), is(1));

        final Optional<StreamStatus> streamStatus = newStreamStatusRepository.find(streamId, source, componentName);
        assertThat(streamStatus.isPresent(), is(true));
        assertThat(streamStatus.get().isUpToDate(), is(false));

        newStreamStatusRepository.setUpToDate(true, streamId, source, componentName);

        final Optional<StreamStatus> updatedStreamStatus = newStreamStatusRepository.find(streamId, source, componentName);
        assertThat(updatedStreamStatus.isPresent(), is(true));
        assertThat(updatedStreamStatus.get().isUpToDate(), is(true));
    }

    @Nested
    class OldestStreamToProcessByAcquiringLockTest {
        private final StreamErrorPersistence streamErrorPersistence = new StreamErrorPersistence();
        private final StreamStatusErrorPersistence streamStatusErrorPersistence = new StreamStatusErrorPersistence();

        @BeforeEach
        void setUp() {
            setField(streamStatusErrorPersistence, "clock", new UtcClock());
            setField(streamErrorPersistence, "streamErrorOccurrencePersistence", new StreamErrorOccurrencePersistence());
            setField(streamErrorPersistence, "streamErrorHashPersistence", new StreamErrorHashPersistence());
        }

        @Test
        public void shouldReturnOldestWhenMultipleStreamsExist() {

            final DataSource viewStoreDataSource = new TestJdbcDataSourceProvider().getViewStoreDataSource(FRAMEWORK);
            when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);

            final UUID streamId1 = randomUUID();
            final UUID streamId2 = randomUUID();
            final String source = "listing";
            final String otherSource = "sjp";
            final String componentName = "event-listener";
            final String otherComponentName = "event-indexer";
            final boolean upToDate = false;
            final ZonedDateTime discoveredAt1 = new UtcClock().now().minusDays(2);
            final ZonedDateTime discoveredAt2 = new UtcClock().now().minusDays(1);
            final long position1 = 5L;
            final long position2 = 10L;
            final long latestKnownPosition1 = 20L;
            final long latestKnownPosition2 = 25L;
            final long latestKnownPosition3 = 30L;
            final long latestKnownPosition4 = 40L;

            assertThat(newStreamStatusRepository.insertIfNotExists(
                    streamId1,
                    source,
                    componentName,
                    discoveredAt1,
                    upToDate), is(1));

            assertThat(newStreamStatusRepository.insertIfNotExists(
                    streamId2,
                    source,
                    componentName,
                    discoveredAt2,
                    upToDate), is(1));

            assertThat(newStreamStatusRepository.insertIfNotExists(
                    streamId1,
                    source,
                    otherComponentName,
                    discoveredAt1,
                    upToDate), is(1));

            assertThat(newStreamStatusRepository.insertIfNotExists(
                    streamId1,
                    otherSource,
                    componentName,
                    discoveredAt1,
                    upToDate), is(1));

            newStreamStatusRepository.updateCurrentPosition(streamId1, source, componentName, position1);
            newStreamStatusRepository.updateCurrentPosition(streamId2, source, componentName, position2);
            newStreamStatusRepository.updateCurrentPosition(streamId1, otherSource, componentName, position1);
            newStreamStatusRepository.updateCurrentPosition(streamId1, source, otherComponentName, position1);

            newStreamStatusRepository.upsertLatestKnownPosition(streamId1, source, componentName, latestKnownPosition1, discoveredAt1);
            newStreamStatusRepository.upsertLatestKnownPosition(streamId2, source, componentName, latestKnownPosition2, discoveredAt2);
            newStreamStatusRepository.upsertLatestKnownPosition(streamId1, otherSource, componentName, latestKnownPosition3, discoveredAt1);
            newStreamStatusRepository.upsertLatestKnownPosition(streamId2, source, otherComponentName, latestKnownPosition4, discoveredAt2);

            final Optional<LockedStreamStatus> result = newStreamStatusRepository.findOldestStreamToProcessByAcquiringLock(source, componentName, MAX_RETRIES);

            assertThat(result.isPresent(), is(true));
            assertThat(result.get().streamId(), is(streamId1));
            assertThat(result.get().position(), is(position1));
            assertThat(result.get().latestKnownPosition(), is(latestKnownPosition1));
            assertThat(result.get().streamErrorId(), is(empty()));
        }

        @Test
        public void shouldReturnEmptyWhenNoStreamFoundToProcess() {

            final DataSource viewStoreDataSource = new TestJdbcDataSourceProvider().getViewStoreDataSource(FRAMEWORK);
            when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);

            final String source = "listing";
            final String componentName = "event-listener";

            final Optional<LockedStreamStatus> result = newStreamStatusRepository.findOldestStreamToProcessByAcquiringLock(source, componentName, MAX_RETRIES);

            assertThat(result, is(empty()));
        }

        @Test
        public void shouldIncludeErroredStreamWhenNoRetryRecordExists() throws Exception {

            final DataSource viewStoreDataSource = new TestJdbcDataSourceProvider().getViewStoreDataSource(FRAMEWORK);
            when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);

            final UUID streamId1 = randomUUID();
            final UUID streamId2 = randomUUID();
            final UUID streamErrorId = randomUUID();
            final String source = "listing";
            final String componentName = "event-listener";
            final boolean upToDate = false;
            final ZonedDateTime discoveredAt = new UtcClock().now();
            final long position = 5L;
            final long latestKnownPosition = 10L;

            assertThat(newStreamStatusRepository.insertIfNotExists(
                    streamId1,
                    source,
                    componentName,
                    discoveredAt.minusDays(2),
                    upToDate), is(1));

            assertThat(newStreamStatusRepository.insertIfNotExists(
                    streamId2,
                    source,
                    componentName,
                    discoveredAt.minusDays(1),
                    upToDate), is(1));

            newStreamStatusRepository.updateCurrentPosition(streamId1, source, componentName, position);
            newStreamStatusRepository.updateCurrentPosition(streamId2, source, componentName, position);
            newStreamStatusRepository.upsertLatestKnownPosition(streamId1, source, componentName, latestKnownPosition, discoveredAt);
            newStreamStatusRepository.upsertLatestKnownPosition(streamId2, source, componentName, latestKnownPosition, discoveredAt);

            final UUID eventId = randomUUID();
            final Long positionInStream = 23L;
            final StreamError streamErrorOnStream1 = aStreamError(streamErrorId, eventId, streamId1, positionInStream, componentName, source);
            try (final Connection connection = viewStoreDataSource.getConnection()) {
                streamErrorPersistence.save(streamErrorOnStream1, connection);
                streamStatusErrorPersistence.markStreamAsErrored(streamId1, streamErrorId, positionInStream, componentName, source, connection);
            }

            final Optional<LockedStreamStatus> result = newStreamStatusRepository.findOldestStreamToProcessByAcquiringLock(source, componentName, MAX_RETRIES);

            assertThat(result.isPresent(), is(true));
            assertThat(result.get().streamId(), is(streamId1));
            assertThat(result.get().streamErrorId().isPresent(), is(true));
            assertThat(result.get().streamErrorId().get(), is(streamErrorId));
        }

        @Test
        public void shouldIncludeErroredStreamWhenRetryCountBelowMaxAndNextRetryTimeInPast() throws Exception {

            final DataSource viewStoreDataSource = new TestJdbcDataSourceProvider().getViewStoreDataSource(FRAMEWORK);
            when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);

            final StreamErrorRetryRepository streamErrorRetryRepository = new StreamErrorRetryRepository();
            setField(streamErrorRetryRepository, "viewStoreJdbcDataSourceProvider", viewStoreJdbcDataSourceProvider);

            final UUID streamId1 = randomUUID();
            final UUID streamErrorId = randomUUID();
            final String source = "listing";
            final String componentName = "event-listener";
            final boolean upToDate = false;
            final ZonedDateTime discoveredAt = new UtcClock().now();
            final long position = 5L;
            final long latestKnownPosition = 10L;

            assertThat(newStreamStatusRepository.insertIfNotExists(
                    streamId1,
                    source,
                    componentName,
                    discoveredAt.minusDays(2),
                    upToDate), is(1));

            newStreamStatusRepository.updateCurrentPosition(streamId1, source, componentName, position);
            newStreamStatusRepository.upsertLatestKnownPosition(streamId1, source, componentName, latestKnownPosition, discoveredAt);

            final UUID eventId = randomUUID();
            final Long positionInStream = 23L;
            final StreamError streamErrorOnStream1 = aStreamError(streamErrorId, eventId, streamId1, positionInStream, componentName, source);
            try (final Connection connection = viewStoreDataSource.getConnection()) {
                streamErrorPersistence.save(streamErrorOnStream1, connection);
                streamStatusErrorPersistence.markStreamAsErrored(streamId1, streamErrorId, positionInStream, componentName, source, connection);
            }

            streamErrorRetryRepository.upsert(new StreamErrorRetry(
                    streamId1,
                    source,
                    componentName,
                    1L,
                    new UtcClock().now().minusHours(2)
            ));

            final Optional<LockedStreamStatus> result = newStreamStatusRepository.findOldestStreamToProcessByAcquiringLock(source, componentName, MAX_RETRIES);

            assertThat(result.isPresent(), is(true));
            assertThat(result.get().streamId(), is(streamId1));
            assertThat(result.get().streamErrorId().isPresent(), is(true));
            assertThat(result.get().streamErrorId().get(), is(streamErrorId));
        }

        @Test
        public void shouldExcludeErroredStreamWhenRetryCountExceedsMax() throws Exception {

            final DataSource viewStoreDataSource = new TestJdbcDataSourceProvider().getViewStoreDataSource(FRAMEWORK);
            when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);

            final StreamErrorRetryRepository streamErrorRetryRepository = new StreamErrorRetryRepository();
            setField(streamErrorRetryRepository, "viewStoreJdbcDataSourceProvider", viewStoreJdbcDataSourceProvider);

            final UUID streamId1 = randomUUID();
            final UUID streamId2 = randomUUID();
            final UUID streamErrorId = randomUUID();
            final String source = "listing";
            final String componentName = "event-listener";
            final boolean upToDate = false;
            final ZonedDateTime discoveredAt = new UtcClock().now();
            final long position = 5L;
            final long latestKnownPosition = 10L;

            assertThat(newStreamStatusRepository.insertIfNotExists(
                    streamId1,
                    source,
                    componentName,
                    discoveredAt.minusDays(2),
                    upToDate), is(1));

            assertThat(newStreamStatusRepository.insertIfNotExists(
                    streamId2,
                    source,
                    componentName,
                    discoveredAt.minusDays(1),
                    upToDate), is(1));

            newStreamStatusRepository.updateCurrentPosition(streamId1, source, componentName, position);
            newStreamStatusRepository.updateCurrentPosition(streamId2, source, componentName, position);
            newStreamStatusRepository.upsertLatestKnownPosition(streamId1, source, componentName, latestKnownPosition, discoveredAt);
            newStreamStatusRepository.upsertLatestKnownPosition(streamId2, source, componentName, latestKnownPosition, discoveredAt);

            final UUID eventId = randomUUID();
            final Long positionInStream = 23L;
            final StreamError streamErrorOnStream1 = aStreamError(streamErrorId, eventId, streamId1, positionInStream, componentName, source);
            try (final Connection connection = viewStoreDataSource.getConnection()) {
                streamErrorPersistence.save(streamErrorOnStream1, connection);
                streamStatusErrorPersistence.markStreamAsErrored(streamId1, streamErrorId, positionInStream, componentName, source, connection);
            }

            streamErrorRetryRepository.upsert(new StreamErrorRetry(
                    streamId1,
                    source,
                    componentName,
                    10L,
                    new UtcClock().now().minusHours(2)
            ));

            final Optional<LockedStreamStatus> result = newStreamStatusRepository.findOldestStreamToProcessByAcquiringLock(source, componentName, MAX_RETRIES);

            assertThat(result.isPresent(), is(true));
            assertThat(result.get().streamId(), is(streamId2));
        }

        @Test
        public void shouldExcludeErroredStreamWhenNextRetryTimeInFuture() throws Exception {

            final DataSource viewStoreDataSource = new TestJdbcDataSourceProvider().getViewStoreDataSource(FRAMEWORK);
            when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);

            final StreamErrorRetryRepository streamErrorRetryRepository = new StreamErrorRetryRepository();
            setField(streamErrorRetryRepository, "viewStoreJdbcDataSourceProvider", viewStoreJdbcDataSourceProvider);

            final UUID streamId1 = randomUUID();
            final UUID streamId2 = randomUUID();
            final UUID streamErrorId = randomUUID();
            final String source = "listing";
            final String componentName = "event-listener";
            final boolean upToDate = false;
            final ZonedDateTime discoveredAt = new UtcClock().now();
            final long position = 5L;
            final long latestKnownPosition = 10L;

            assertThat(newStreamStatusRepository.insertIfNotExists(
                    streamId1,
                    source,
                    componentName,
                    discoveredAt.minusDays(2),
                    upToDate), is(1));

            assertThat(newStreamStatusRepository.insertIfNotExists(
                    streamId2,
                    source,
                    componentName,
                    discoveredAt.minusDays(1),
                    upToDate), is(1));

            newStreamStatusRepository.updateCurrentPosition(streamId1, source, componentName, position);
            newStreamStatusRepository.updateCurrentPosition(streamId2, source, componentName, position);
            newStreamStatusRepository.upsertLatestKnownPosition(streamId1, source, componentName, latestKnownPosition, discoveredAt);
            newStreamStatusRepository.upsertLatestKnownPosition(streamId2, source, componentName, latestKnownPosition, discoveredAt);

            final UUID eventId = randomUUID();
            final Long positionInStream = 23L;
            final StreamError streamErrorOnStream1 = aStreamError(streamErrorId, eventId, streamId1, positionInStream, componentName, source);
            try (final Connection connection = viewStoreDataSource.getConnection()) {
                streamErrorPersistence.save(streamErrorOnStream1, connection);
                streamStatusErrorPersistence.markStreamAsErrored(streamId1, streamErrorId, positionInStream, componentName, source, connection);
            }

            streamErrorRetryRepository.upsert(new StreamErrorRetry(
                    streamId1,
                    source,
                    componentName,
                    1L,
                    new UtcClock().now().plusHours(2)
            ));

            final Optional<LockedStreamStatus> result = newStreamStatusRepository.findOldestStreamToProcessByAcquiringLock(source, componentName, MAX_RETRIES);

            assertThat(result.isPresent(), is(true));
            assertThat(result.get().streamId(), is(streamId2));
        }

        private StreamError aStreamError(
                final UUID streamErrorId,
                final UUID eventId,
                final UUID streamId,
                final Long positionInStream,
                final String componentName,
                final String source) {

            final String hash = "9374874397kjshdkfjhsdf";
            final StreamErrorHash streamErrorHash = new StreamErrorHash(
                    hash,
                    "some.exception.ClassName",
                    empty(),
                    "some.java.ClassName",
                    "someMethod",
                    2334
            );

            final UtcClock utcClock = new UtcClock();
            final StreamErrorOccurrence streamErrorOccurrence = new StreamErrorOccurrence(
                    streamErrorId,
                    hash,
                    "some-exception-message",
                    empty(),
                    "events.context.some-event-name",
                    eventId,
                    streamId,
                    positionInStream,
                    utcClock.now(),
                    "stack-trace",
                    componentName,
                    source,
                    utcClock.now()
            );

            return new StreamError(
                    streamErrorOccurrence,
                    streamErrorHash
            );
        }

        @Test
        public void shouldPickUpAndSkipStreamsAccordingToErrorRetryTruthTable() throws Exception {

            final DataSource viewStoreDataSource = new TestJdbcDataSourceProvider().getViewStoreDataSource(FRAMEWORK);
            when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);

            final StreamErrorRetryRepository streamErrorRetryRepository = new StreamErrorRetryRepository();
            setField(streamErrorRetryRepository, "viewStoreJdbcDataSourceProvider", viewStoreJdbcDataSourceProvider);

            final String source = "listing";
            final String componentName = "event-listener";
            final ZonedDateTime now = new UtcClock().now();
            final long position = 5L;
            final long latestKnownPosition = 10L;

            // Truth table streams ordered by discovered_at (oldest first):
            // Row 1: healthy, no retry entry              → PICKED UP
            // Row 2: healthy, leftover retry entry         → PICKED UP
            // Row 3: errored, no retry entry               → PICKED UP
            // Row 4: errored, retry eligible (count<max, time past)  → PICKED UP
            // Row 5: errored, retry not yet (count<max, time future) → SKIPPED
            // Row 6: errored, retries exhausted (count>=max)         → SKIPPED

            final UUID streamA = randomUUID();
            final UUID streamB = randomUUID();
            final UUID streamC = randomUUID();
            final UUID streamD = randomUUID();
            final UUID streamE = randomUUID();
            final UUID streamF = randomUUID();

            final List<UUID> allStreams = List.of(streamA, streamB, streamC, streamD, streamE, streamF);

            final UUID errorIdC = randomUUID();
            final UUID errorIdD = randomUUID();
            final UUID errorIdE = randomUUID();
            final UUID errorIdF = randomUUID();

            // Create all 6 streams with staggered discovered_at, all behind
            for (int i = 0; i < allStreams.size(); i++) {
                final UUID streamId = allStreams.get(i);
                newStreamStatusRepository.insertIfNotExists(streamId, source, componentName, now.minusDays(6 - i), false);
                newStreamStatusRepository.updateCurrentPosition(streamId, source, componentName, position);
                newStreamStatusRepository.upsertLatestKnownPosition(streamId, source, componentName, latestKnownPosition, now);
            }

            // Row 2: leftover retry entry on healthy stream (no error set)
            streamErrorRetryRepository.upsert(new StreamErrorRetry(streamB, source, componentName, 1L, now.minusHours(1)));

            // Rows 3-6: mark as errored
            final Map<UUID, UUID> erroredStreams = Map.of(streamC, errorIdC, streamD, errorIdD, streamE, errorIdE, streamF, errorIdF);
            try (final Connection connection = viewStoreDataSource.getConnection()) {
                for (final var entry : erroredStreams.entrySet()) {
                    streamErrorPersistence.save(aStreamError(entry.getValue(), randomUUID(), entry.getKey(), 6L, componentName, source), connection);
                    streamStatusErrorPersistence.markStreamAsErrored(entry.getKey(), entry.getValue(), 6L, componentName, source, connection);
                }
            }

            // Row 4: eligible retry (count below max, time in past)
            streamErrorRetryRepository.upsert(new StreamErrorRetry(streamD, source, componentName, 2L, now.minusHours(1)));
            // Row 5: retry not yet due (count below max, time in future)
            streamErrorRetryRepository.upsert(new StreamErrorRetry(streamE, source, componentName, 2L, now.plusHours(2)));
            // Row 6: retries exhausted (count >= max)
            streamErrorRetryRepository.upsert(new StreamErrorRetry(streamF, source, componentName, 10L, now.minusHours(1)));

            // Verify total eligible count = 4
            assertThat(newStreamStatusRepository.countStreamsHavingEventsToProcess(source, componentName, MAX_RETRIES, 50), is(4));

            // Consume eligible streams in discovered_at order, verify each, then confirm skipped streams leave empty
            for (final var expected : List.of(
                    Map.entry(streamA, Optional.<UUID>empty()),
                    Map.entry(streamB, Optional.<UUID>empty()),
                    Map.entry(streamC, Optional.of(errorIdC)),
                    Map.entry(streamD, Optional.of(errorIdD)))) {

                final Optional<LockedStreamStatus> result = newStreamStatusRepository.findOldestStreamToProcessByAcquiringLock(source, componentName, MAX_RETRIES);
                assertThat(result.isPresent(), is(true));
                assertThat(result.get().streamId(), is(expected.getKey()));
                assertThat(result.get().streamErrorId(), is(expected.getValue()));
                newStreamStatusRepository.updateCurrentPosition(expected.getKey(), source, componentName, latestKnownPosition);
            }

            // streamE (future retry) and streamF (exhausted) both skipped
            assertThat(newStreamStatusRepository.findOldestStreamToProcessByAcquiringLock(source, componentName, MAX_RETRIES), is(empty()));
            assertThat(newStreamStatusRepository.countStreamsHavingEventsToProcess(source, componentName, MAX_RETRIES, 50), is(0));
        }

        @Test
        public void shouldExcludeStreamsWherePositionEqualsLatestKnownPosition() {

            final DataSource viewStoreDataSource = new TestJdbcDataSourceProvider().getViewStoreDataSource(FRAMEWORK);
            when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);

            final UUID streamId1 = randomUUID();
            final UUID streamId2 = randomUUID();
            final String source = "listing";
            final String componentName = "event-listener";
            final boolean upToDate = false;
            final ZonedDateTime discoveredAt = new UtcClock().now().minusDays(1);
            final long position1 = 10L;
            final long latestKnownPosition1 = 10L;
            final long position2 = 5L;
            final long latestKnownPosition2 = 15L;

            assertThat(newStreamStatusRepository.insertIfNotExists(
                    streamId1,
                    source,
                    componentName,
                    discoveredAt,
                    upToDate), is(1));

            assertThat(newStreamStatusRepository.insertIfNotExists(
                    streamId2,
                    source,
                    componentName,
                    discoveredAt,
                    upToDate), is(1));

            newStreamStatusRepository.updateCurrentPosition(streamId1, source, componentName, position1);
            newStreamStatusRepository.updateCurrentPosition(streamId2, source, componentName, position2);
            newStreamStatusRepository.upsertLatestKnownPosition(streamId1, source, componentName, latestKnownPosition1, discoveredAt);
            newStreamStatusRepository.upsertLatestKnownPosition(streamId2, source, componentName, latestKnownPosition2, discoveredAt);

            final Optional<LockedStreamStatus> result = newStreamStatusRepository.findOldestStreamToProcessByAcquiringLock(source, componentName, MAX_RETRIES);

            assertThat(result.isPresent(), is(true));
            assertThat(result.get().streamId(), is(streamId2));
        }
    }
}
