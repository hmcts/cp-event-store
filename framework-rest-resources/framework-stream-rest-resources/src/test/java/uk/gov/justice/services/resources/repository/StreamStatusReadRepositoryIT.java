package uk.gov.justice.services.resources.repository;

import static java.util.Optional.empty;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorOccurrence;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorOccurrencePersistence;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorDetailsRowMapper;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorHash;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorHashPersistence;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorHashRowMapper;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamStatusErrorPersistence;
import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRepository;
import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRowMapper;
import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamStatus;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;
import uk.gov.justice.services.test.utils.persistence.TestJdbcDataSourceProvider;

import java.time.ZonedDateTime;
import java.util.List;
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
public class StreamStatusReadRepositoryIT {

    private static final String FRAMEWORK = "framework";
    private static final String LISTING_SOURCE = "listing";
    private static final String EVENT_LISTENER_COMPONENT = "EVENT_LISTENER";
    private static final String EVENT_INDEXER_COMPONENT = "EVENT_INDEXER";
    private static final UUID STREAM_1_ID = randomUUID();
    private static final UUID STREAM_2_ID = randomUUID();
    private static final UUID STREAM_3_ID = randomUUID();
    private static final UUID STREAM_4_ID = randomUUID();
    private static final UUID STREAM_5_ID = randomUUID();
    private static final ZonedDateTime BASE_DISCOVERED_AT = new UtcClock().now().minusDays(2);
    private static final ZonedDateTime STREAM_1_LISTENER_DISCOVERED_AT = BASE_DISCOVERED_AT.plusSeconds(1);
    private static final ZonedDateTime STREAM_1_INDEXER_DISCOVERED_AT = BASE_DISCOVERED_AT.plusSeconds(2);
    private static final ZonedDateTime STREAM_2_LISTENER_DISCOVERED_AT = BASE_DISCOVERED_AT.plusSeconds(3);
    private static final ZonedDateTime STREAM_2_INDEXER_DISCOVERED_AT = BASE_DISCOVERED_AT.plusSeconds(4);
    private static final ZonedDateTime STREAM_3_DISCOVERED_AT = BASE_DISCOVERED_AT.plusSeconds(5);
    private static final ZonedDateTime STREAM_4_LISTENER_DISCOVERED_AT = BASE_DISCOVERED_AT.plusMinutes(1);
    private static final ZonedDateTime STREAM_4_INDEXER_DISCOVERED_AT = BASE_DISCOVERED_AT.plusMinutes(2);
    private static final ZonedDateTime STREAM_5_DISCOVERED_AT = BASE_DISCOVERED_AT.plusMinutes(3);
    private static final String ERROR_1_HASH = "hash-1";
    private static final String ERROR_2_HASH = "hash-2";
    final StreamErrorOccurrence STREAM_1_LISTENER_ERROR_1 = new StreamErrorOccurrence(
            randomUUID(), ERROR_1_HASH, "some-exception-message", empty(),
            "event-name", randomUUID(), STREAM_1_ID, 1L,
            new UtcClock().now(), "stack-trace",
            EVENT_LISTENER_COMPONENT, LISTING_SOURCE, new UtcClock().now()
    );
    final StreamErrorOccurrence STREAM_1_INDEXER_ERROR_1 = new StreamErrorOccurrence(
            randomUUID(), ERROR_1_HASH, "some-exception-message", empty(),
            "event-name", randomUUID(), STREAM_1_ID, 1L,
            new UtcClock().now(), "stack-trace",
            EVENT_INDEXER_COMPONENT, LISTING_SOURCE, new UtcClock().now()
    );
    final StreamErrorOccurrence STREAM_2_LISTENER_ERROR_1 = new StreamErrorOccurrence(
            randomUUID(), ERROR_1_HASH, "some-exception-message", empty(),
            "event-name", randomUUID(), STREAM_2_ID, 1L,
            new UtcClock().now(), "stack-trace",
            EVENT_LISTENER_COMPONENT, LISTING_SOURCE, new UtcClock().now()
    );
    final StreamErrorOccurrence STREAM_2_INDEXER_ERROR_2 = new StreamErrorOccurrence(
            randomUUID(), ERROR_2_HASH, "some-exception-message", empty(),
            "event-name", randomUUID(), STREAM_2_ID, 1L,
            new UtcClock().now(), "stack-trace",
            EVENT_INDEXER_COMPONENT, LISTING_SOURCE, new UtcClock().now()
    );
    final StreamErrorOccurrence STREAM_3_ERROR_1 = new StreamErrorOccurrence(
            randomUUID(), ERROR_1_HASH, "some-exception-message", empty(),
            "event-name", randomUUID(), STREAM_3_ID, 1L,
            new UtcClock().now(), "stack-trace",
            EVENT_LISTENER_COMPONENT, LISTING_SOURCE, new UtcClock().now()
    );

    @Mock
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    @InjectMocks
    private NewStreamStatusRepository newStreamStatusRepository;

    @Spy
    private UtcClock utcClock;
    @InjectMocks
    private StreamStatusErrorPersistence streamStatusErrorPersistence;

    @Spy
    private StreamErrorHashRowMapper streamErrorHashRowMapper;
    @InjectMocks
    private StreamErrorHashPersistence streamErrorHashPersistence;

    @Spy
    private StreamErrorDetailsRowMapper streamErrorDetailsRowMapper;
    @InjectMocks
    private StreamErrorOccurrencePersistence streamErrorOccurrencePersistence;

    @Spy
    private NewStreamStatusRowMapper streamStatusRowMapper;
    @InjectMocks
    private StreamStatusReadRepository streamStatusReadRepository;

    @BeforeEach
    public void cleanDatabase() throws Exception {
        new DatabaseCleaner().cleanViewStoreTables(FRAMEWORK, "stream_error", "stream_error_hash", "stream_status");
        setupStreams();
    }

    private void setupStreams() throws Exception {
        final DataSource viewStoreDataSource = new TestJdbcDataSourceProvider().getViewStoreDataSource(FRAMEWORK);
        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);

        assertThat(streamStatusReadRepository.findByErrorHash(ERROR_1_HASH).isEmpty(), is(true));
        assertThat(streamStatusReadRepository.findByStreamId(STREAM_1_ID).isEmpty(), is(true));
        assertThat(streamStatusReadRepository.findErrorStreams().isEmpty(), is(true));

        newStreamStatusRepository.insertIfNotExists(STREAM_1_ID, LISTING_SOURCE, EVENT_LISTENER_COMPONENT, STREAM_1_LISTENER_DISCOVERED_AT, false);
        newStreamStatusRepository.insertIfNotExists(STREAM_1_ID, LISTING_SOURCE, EVENT_INDEXER_COMPONENT, STREAM_1_INDEXER_DISCOVERED_AT, false);
        newStreamStatusRepository.insertIfNotExists(STREAM_2_ID, LISTING_SOURCE, EVENT_LISTENER_COMPONENT, STREAM_2_LISTENER_DISCOVERED_AT, false);
        newStreamStatusRepository.insertIfNotExists(STREAM_2_ID, LISTING_SOURCE, EVENT_INDEXER_COMPONENT, STREAM_2_INDEXER_DISCOVERED_AT, false);
        newStreamStatusRepository.insertIfNotExists(STREAM_3_ID, LISTING_SOURCE, EVENT_LISTENER_COMPONENT, STREAM_3_DISCOVERED_AT, false);
        newStreamStatusRepository.insertIfNotExists(STREAM_4_ID, LISTING_SOURCE, EVENT_LISTENER_COMPONENT, STREAM_4_LISTENER_DISCOVERED_AT, false);
        newStreamStatusRepository.insertIfNotExists(STREAM_4_ID, LISTING_SOURCE, EVENT_INDEXER_COMPONENT, STREAM_4_INDEXER_DISCOVERED_AT, false);
        newStreamStatusRepository.insertIfNotExists(STREAM_5_ID, LISTING_SOURCE, EVENT_LISTENER_COMPONENT, STREAM_5_DISCOVERED_AT, false);

        insertEntriesToStreamErrorHash(ERROR_1_HASH, ERROR_2_HASH, viewStoreDataSource);

        //errors only for streams 1-3
        streamErrorOccurrencePersistence.insert(STREAM_1_LISTENER_ERROR_1, viewStoreDataSource.getConnection());
        streamErrorOccurrencePersistence.insert(STREAM_1_INDEXER_ERROR_1, viewStoreDataSource.getConnection());
        streamErrorOccurrencePersistence.insert(STREAM_2_LISTENER_ERROR_1, viewStoreDataSource.getConnection());
        streamErrorOccurrencePersistence.insert(STREAM_2_INDEXER_ERROR_2, viewStoreDataSource.getConnection());
        streamErrorOccurrencePersistence.insert(STREAM_3_ERROR_1, viewStoreDataSource.getConnection());

        streamStatusErrorPersistence.markStreamAsErrored(STREAM_1_ID, STREAM_1_LISTENER_ERROR_1.id(), 1L, STREAM_1_LISTENER_ERROR_1.componentName(),
                STREAM_1_LISTENER_ERROR_1.source(), viewStoreDataSource.getConnection());
        streamStatusErrorPersistence.markStreamAsErrored(STREAM_1_ID, STREAM_1_INDEXER_ERROR_1.id(), 1L, STREAM_1_INDEXER_ERROR_1.componentName(),
                STREAM_1_INDEXER_ERROR_1.source(), viewStoreDataSource.getConnection());
        streamStatusErrorPersistence.markStreamAsErrored(STREAM_2_ID, STREAM_2_LISTENER_ERROR_1.id(), 1L, STREAM_2_LISTENER_ERROR_1.componentName(),
                STREAM_2_INDEXER_ERROR_2.source(), viewStoreDataSource.getConnection());
        streamStatusErrorPersistence.markStreamAsErrored(STREAM_2_ID, STREAM_2_INDEXER_ERROR_2.id(), 1L, STREAM_2_INDEXER_ERROR_2.componentName(),
                STREAM_2_INDEXER_ERROR_2.source(), viewStoreDataSource.getConnection());
        streamStatusErrorPersistence.markStreamAsErrored(STREAM_3_ID, STREAM_3_ERROR_1.id(), 1L, STREAM_3_ERROR_1.componentName(),
                STREAM_3_ERROR_1.source(), viewStoreDataSource.getConnection());
    }

    @Test
    public void shouldQueryAllStreamsInDescOrderOfUpdatedAtByErrorHash() {
        final List<StreamStatus> streamStatuses = streamStatusReadRepository.findByErrorHash(ERROR_1_HASH);

        assertThat(streamStatuses.size(), is(4));
        final StreamStatus streamStatus1 = streamStatuses.get(0);
        assertThat(streamStatus1.streamId(), is(STREAM_3_ID));
        assertThat(streamStatus1.latestKnownPosition(), is(0L));
        assertThat(streamStatus1.position(), is(0L));
        assertThat(streamStatus1.component(), is(EVENT_LISTENER_COMPONENT));
        assertThat(streamStatus1.source(), is(LISTING_SOURCE));
        assertThat(streamStatus1.isUpToDate(), is(false));
        assertThat(streamStatus1.streamErrorId().get(), is(STREAM_3_ERROR_1.id()));
        assertThat(streamStatus1.streamErrorPosition().get(), is(1L));
        assertNotNull(streamStatus1.discoveredAt());

        final StreamStatus streamStatus2 = streamStatuses.get(1);
        assertThat(streamStatus2.streamId(), is(STREAM_2_ID));
        assertThat(streamStatus2.latestKnownPosition(), is(0L));
        assertThat(streamStatus2.position(), is(0L));
        assertThat(streamStatus2.component(), is(EVENT_LISTENER_COMPONENT));
        assertThat(streamStatus2.source(), is(LISTING_SOURCE));
        assertThat(streamStatus2.isUpToDate(), is(false));
        assertThat(streamStatus2.streamErrorId().get(), is(STREAM_2_LISTENER_ERROR_1.id()));
        assertThat(streamStatus2.streamErrorPosition().get(), is(1L));
        assertNotNull(streamStatus2.discoveredAt());

        final StreamStatus streamStatus3 = streamStatuses.get(2);
        assertThat(streamStatus3.streamId(), is(STREAM_1_ID));
        assertThat(streamStatus3.latestKnownPosition(), is(0L));
        assertThat(streamStatus3.position(), is(0L));
        assertThat(streamStatus3.component(), is(EVENT_INDEXER_COMPONENT));
        assertThat(streamStatus3.source(), is(LISTING_SOURCE));
        assertThat(streamStatus3.isUpToDate(), is(false));
        assertThat(streamStatus3.streamErrorId().get(), is(STREAM_1_INDEXER_ERROR_1.id()));
        assertThat(streamStatus3.streamErrorPosition().get(), is(1L));
        assertNotNull(streamStatus3.discoveredAt());

        final StreamStatus streamStatus4 = streamStatuses.get(3);
        assertThat(streamStatus4.streamId(), is(STREAM_1_ID));
        assertThat(streamStatus4.latestKnownPosition(), is(0L));
        assertThat(streamStatus4.position(), is(0L));
        assertThat(streamStatus4.component(), is(EVENT_LISTENER_COMPONENT));
        assertThat(streamStatus4.source(), is(LISTING_SOURCE));
        assertThat(streamStatus4.isUpToDate(), is(false));
        assertThat(streamStatus4.streamErrorId().get(), is(STREAM_1_LISTENER_ERROR_1.id()));
        assertThat(streamStatus4.streamErrorPosition().get(), is(1L));
        assertNotNull(streamStatus4.discoveredAt());
    }

    @Test
    public void shouldQueryAllStreamsInDescOrderOfUpdatedAtByStreamId() {
        final List<StreamStatus> streamStatuses = streamStatusReadRepository.findByStreamId(STREAM_1_ID);

        assertThat(streamStatuses.size(), is(2));
        final StreamStatus streamStatus1 = streamStatuses.get(0);
        assertThat(streamStatus1.streamId(), is(STREAM_1_ID));
        assertThat(streamStatus1.latestKnownPosition(), is(0L));
        assertThat(streamStatus1.position(), is(0L));
        assertThat(streamStatus1.component(), is(EVENT_INDEXER_COMPONENT));
        assertThat(streamStatus1.source(), is(LISTING_SOURCE));
        assertThat(streamStatus1.isUpToDate(), is(false));
        assertThat(streamStatus1.streamErrorId().get(), is(STREAM_1_INDEXER_ERROR_1.id()));
        assertThat(streamStatus1.streamErrorPosition().get(), is(1L));
        assertNotNull(streamStatus1.discoveredAt());

        final StreamStatus streamStatus2 = streamStatuses.get(1);
        assertThat(streamStatus2.streamId(), is(STREAM_1_ID));
        assertThat(streamStatus2.latestKnownPosition(), is(0L));
        assertThat(streamStatus2.position(), is(0L));
        assertThat(streamStatus2.component(), is(EVENT_LISTENER_COMPONENT));
        assertThat(streamStatus2.source(), is(LISTING_SOURCE));
        assertThat(streamStatus2.isUpToDate(), is(false));
        assertThat(streamStatus2.streamErrorId().get(), is(STREAM_1_LISTENER_ERROR_1.id()));
        assertThat(streamStatus2.streamErrorPosition().get(), is(1L));
        assertNotNull(streamStatus2.discoveredAt());
    }

    @Test
    public void shouldQueryAllErroredStreamsInDescOrderOfUpdatedAt() {
        final List<StreamStatus> streamStatuses = streamStatusReadRepository.findErrorStreams();

        assertThat(streamStatuses.size(), is(5));
        final StreamStatus streamStatus1 = streamStatuses.get(0);
        assertThat(streamStatus1.streamId(), is(STREAM_3_ID));
        assertThat(streamStatus1.component(), is(EVENT_LISTENER_COMPONENT));
        assertThat(streamStatus1.source(), is(LISTING_SOURCE));
        assertThat(streamStatus1.isUpToDate(), is(false));
        assertThat(streamStatus1.streamErrorId().get(), is(STREAM_3_ERROR_1.id()));
        assertThat(streamStatus1.streamErrorPosition().get(), is(1L));
        assertNotNull(streamStatus1.discoveredAt());

        final StreamStatus streamStatus2 = streamStatuses.get(1);
        assertThat(streamStatus2.streamId(), is(STREAM_2_ID));
        assertThat(streamStatus2.latestKnownPosition(), is(0L));
        assertThat(streamStatus2.position(), is(0L));
        assertThat(streamStatus2.component(), is(EVENT_INDEXER_COMPONENT));
        assertThat(streamStatus2.source(), is(LISTING_SOURCE));
        assertThat(streamStatus2.isUpToDate(), is(false));
        assertThat(streamStatus2.streamErrorId().get(), is(STREAM_2_INDEXER_ERROR_2.id()));
        assertThat(streamStatus2.streamErrorPosition().get(), is(1L));
        assertNotNull(streamStatus2.discoveredAt());

        final StreamStatus streamStatus3 = streamStatuses.get(2);
        assertThat(streamStatus3.streamId(), is(STREAM_2_ID));
        assertThat(streamStatus3.latestKnownPosition(), is(0L));
        assertThat(streamStatus3.position(), is(0L));
        assertThat(streamStatus3.component(), is(EVENT_LISTENER_COMPONENT));
        assertThat(streamStatus3.source(), is(LISTING_SOURCE));
        assertThat(streamStatus3.isUpToDate(), is(false));
        assertThat(streamStatus3.streamErrorId().get(), is(STREAM_2_LISTENER_ERROR_1.id()));
        assertThat(streamStatus3.streamErrorPosition().get(), is(1L));
        assertNotNull(streamStatus3.discoveredAt());

        final StreamStatus streamStatus4 = streamStatuses.get(3);
        assertThat(streamStatus4.streamId(), is(STREAM_1_ID));
        assertThat(streamStatus4.latestKnownPosition(), is(0L));
        assertThat(streamStatus4.position(), is(0L));
        assertThat(streamStatus4.component(), is(EVENT_INDEXER_COMPONENT));
        assertThat(streamStatus4.source(), is(LISTING_SOURCE));
        assertThat(streamStatus4.isUpToDate(), is(false));
        assertThat(streamStatus4.streamErrorId().get(), is(STREAM_1_INDEXER_ERROR_1.id()));
        assertThat(streamStatus4.streamErrorPosition().get(), is(1L));
        assertNotNull(streamStatus4.discoveredAt());

        final StreamStatus streamStatus5 = streamStatuses.get(4);
        assertThat(streamStatus5.streamId(), is(STREAM_1_ID));
        assertThat(streamStatus5.latestKnownPosition(), is(0L));
        assertThat(streamStatus5.position(), is(0L));
        assertThat(streamStatus5.component(), is(EVENT_LISTENER_COMPONENT));
        assertThat(streamStatus5.source(), is(LISTING_SOURCE));
        assertThat(streamStatus5.isUpToDate(), is(false));
        assertThat(streamStatus5.streamErrorId().get(), is(STREAM_1_LISTENER_ERROR_1.id()));
        assertThat(streamStatus5.streamErrorPosition().get(), is(1L));
        assertNotNull(streamStatus5.discoveredAt());
    }

    private void insertEntriesToStreamErrorHash(String error1Hash, String error2Hash, DataSource dataSource) throws Exception {
        final StreamErrorHash streamError1Hash = new StreamErrorHash(error1Hash, "java.lang.NullPointerException", Optional.empty(), "java.lang.NullPointerException", "find", 1);
        final StreamErrorHash streamError2Hash = new StreamErrorHash(error2Hash, "java.lang.IllegalArgumentException", Optional.empty(), "java.lang.IllegalArgumentException", "find1", 2);
        streamErrorHashPersistence.upsert(streamError1Hash, dataSource.getConnection());
        streamErrorHashPersistence.upsert(streamError2Hash, dataSource.getConnection());
    }

    private void insertEntryToStreamError(final UUID stream1Id, final String error1Hash,
                                          final Long position, final DataSource dataSource,
                                          final String source, final String component) throws Exception {
        final StreamErrorOccurrence streamErrorOccurrence = new StreamErrorOccurrence(
                randomUUID(), error1Hash, "some-exception-message", empty(),
                "event-name", randomUUID(), stream1Id, position,
                new UtcClock().now(), "stack-trace",
                component, source, new UtcClock().now()
        );
        streamErrorOccurrencePersistence.insert(streamErrorOccurrence, dataSource.getConnection());
    }
}
