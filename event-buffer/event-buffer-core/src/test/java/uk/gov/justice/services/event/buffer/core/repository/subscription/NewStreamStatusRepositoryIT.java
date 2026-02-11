package uk.gov.justice.services.event.buffer.core.repository.subscription;

import static java.util.Optional.empty;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.test.utils.core.reflection.ReflectionUtil.setField;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamError;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorHash;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorHashPersistence;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorOccurrence;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorOccurrencePersistence;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorPersistence;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamStatusErrorPersistence;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;
import uk.gov.justice.services.test.utils.persistence.TestJdbcDataSourceProvider;

import java.sql.Connection;
import java.time.ZonedDateTime;
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
        final DatabaseCleaner databaseCleaner = new DatabaseCleaner();
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
    public void shouldUpdateLatestPosition() throws Exception {

        final DataSource viewStoreDataSource = new TestJdbcDataSourceProvider().getViewStoreDataSource(FRAMEWORK);
        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);

        final UUID streamId = randomUUID();

        final String source = "some-source";
        final String componentName = "some-component-name";
        final boolean upToDate = true;
        final ZonedDateTime updatedAt = new UtcClock().now();
        final long latestKnownPosition = 23L;

        assertThat(newStreamStatusRepository.findAll().isEmpty(), is(true));

        assertThat(newStreamStatusRepository.insertIfNotExists(
                streamId,
                source,
                componentName,
                updatedAt,
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
                updatedAt
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
            final ZonedDateTime updatedAt1 = new UtcClock().now().minusDays(2);
            final ZonedDateTime updatedAt2 = new UtcClock().now().minusDays(1);
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
                    updatedAt1,
                    upToDate), is(1));

            assertThat(newStreamStatusRepository.insertIfNotExists(
                    streamId2,
                    source,
                    componentName,
                    updatedAt2,
                    upToDate), is(1));

            assertThat(newStreamStatusRepository.insertIfNotExists(
                    streamId1,
                    source,
                    otherComponentName,
                    updatedAt1,
                    upToDate), is(1));

            assertThat(newStreamStatusRepository.insertIfNotExists(
                    streamId1,
                    otherSource,
                    componentName,
                    updatedAt1,
                    upToDate), is(1));

            newStreamStatusRepository.updateCurrentPosition(streamId1, source, componentName, position1);
            newStreamStatusRepository.updateCurrentPosition(streamId2, source, componentName, position2);
            newStreamStatusRepository.updateCurrentPosition(streamId1, otherSource, componentName, position1);
            newStreamStatusRepository.updateCurrentPosition(streamId1, source, otherComponentName, position1);

            newStreamStatusRepository.upsertLatestKnownPosition(streamId1, source, componentName, latestKnownPosition1, updatedAt1);
            newStreamStatusRepository.upsertLatestKnownPosition(streamId2, source, componentName, latestKnownPosition2, updatedAt2);
            newStreamStatusRepository.upsertLatestKnownPosition(streamId1, otherSource, componentName, latestKnownPosition3, updatedAt1);
            newStreamStatusRepository.upsertLatestKnownPosition(streamId2, source, otherComponentName, latestKnownPosition4, updatedAt2);

            final Optional<LockedStreamStatus> result = newStreamStatusRepository.findOldestStreamToProcessByAcquiringLock(source, componentName);

            assertThat(result.isPresent(), is(true));
            assertThat(result.get().streamId(), is(streamId1));
            assertThat(result.get().position(), is(position1));
            assertThat(result.get().latestKnownPosition(), is(latestKnownPosition1));
        }

        @Test
        public void shouldReturnEmptyWhenNoStreamFoundToProcess() {

            final DataSource viewStoreDataSource = new TestJdbcDataSourceProvider().getViewStoreDataSource(FRAMEWORK);
            when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);

            final String source = "listing";
            final String componentName = "event-listener";

            final Optional<LockedStreamStatus> result = newStreamStatusRepository.findOldestStreamToProcessByAcquiringLock(source, componentName);

            assertThat(result, is(empty()));
        }

        @Test
        public void shouldExcludeStreamsWithErrors() throws Exception {

            final DataSource viewStoreDataSource = new TestJdbcDataSourceProvider().getViewStoreDataSource(FRAMEWORK);
            when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);

            final UUID streamId1 = randomUUID();
            final UUID streamId2 = randomUUID();
            final UUID streamErrorId = randomUUID();
            final String source = "listing";
            final String componentName = "event-listener";
            final boolean upToDate = false;
            final ZonedDateTime updatedAt = new UtcClock().now();
            final long position = 5L;
            final long latestKnownPosition = 10L;

            assertThat(newStreamStatusRepository.insertIfNotExists(
                    streamId1,
                    source,
                    componentName,
                    updatedAt.minusDays(2),
                    upToDate), is(1));

            assertThat(newStreamStatusRepository.insertIfNotExists(
                    streamId2,
                    source,
                    componentName,
                    updatedAt.minusDays(1),
                    upToDate), is(1));

            newStreamStatusRepository.updateCurrentPosition(streamId1, source, componentName, position);
            newStreamStatusRepository.updateCurrentPosition(streamId2, source, componentName, position);
            newStreamStatusRepository.upsertLatestKnownPosition(streamId1, source, componentName, latestKnownPosition, updatedAt);
            newStreamStatusRepository.upsertLatestKnownPosition(streamId2, source, componentName, latestKnownPosition, updatedAt);

            final UUID eventId = randomUUID();
            final Long positionInStream = 23L;
            final StreamError streamErrorOnStream1 = aStreamError(streamErrorId, eventId, streamId1, positionInStream, componentName, source);
            try (final Connection connection = viewStoreDataSource.getConnection()) {
                streamErrorPersistence.save(streamErrorOnStream1, connection);
                streamStatusErrorPersistence.markStreamAsErrored(streamId1, streamErrorId, positionInStream, componentName, source, connection);
            }


            final Optional<LockedStreamStatus> result = newStreamStatusRepository.findOldestStreamToProcessByAcquiringLock(source, componentName);

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

            final StreamErrorOccurrence streamErrorOccurrence = new StreamErrorOccurrence(
                    streamErrorId,
                    hash,
                    "some-exception-message",
                    empty(),
                    "events.context.some-event-name",
                    eventId,
                    streamId,
                    positionInStream,
                    new UtcClock().now(),
                    "stack-trace",
                    componentName,
                    source
            );

            return new StreamError(
                    streamErrorOccurrence,
                    streamErrorHash
            );
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
            final ZonedDateTime updatedAt = new UtcClock().now().minusDays(1);
            final long position1 = 10L;
            final long latestKnownPosition1 = 10L;
            final long position2 = 5L;
            final long latestKnownPosition2 = 15L;

            assertThat(newStreamStatusRepository.insertIfNotExists(
                    streamId1,
                    source,
                    componentName,
                    updatedAt,
                    upToDate), is(1));

            assertThat(newStreamStatusRepository.insertIfNotExists(
                    streamId2,
                    source,
                    componentName,
                    updatedAt,
                    upToDate), is(1));

            newStreamStatusRepository.updateCurrentPosition(streamId1, source, componentName, position1);
            newStreamStatusRepository.updateCurrentPosition(streamId2, source, componentName, position2);
            newStreamStatusRepository.upsertLatestKnownPosition(streamId1, source, componentName, latestKnownPosition1, updatedAt);
            newStreamStatusRepository.upsertLatestKnownPosition(streamId2, source, componentName, latestKnownPosition2, updatedAt);

            final Optional<LockedStreamStatus> result = newStreamStatusRepository.findOldestStreamToProcessByAcquiringLock(source, componentName);

            assertThat(result.isPresent(), is(true));
            assertThat(result.get().streamId(), is(streamId2));
        }
    }
}
