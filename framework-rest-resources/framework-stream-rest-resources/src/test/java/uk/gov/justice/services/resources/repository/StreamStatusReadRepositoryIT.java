package uk.gov.justice.services.resources.repository;

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
import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorDetails;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorDetailsPersistence;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorHash;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorHashPersistence;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamStatusErrorPersistence;
import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRepository;
import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRowMapper;
import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamStatus;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;
import uk.gov.justice.services.test.utils.persistence.TestJdbcDataSourceProvider;

import static java.util.Optional.empty;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

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
    private static final ZonedDateTime UPDATED_AT = new UtcClock().now().minusDays(2);
    private static final ZonedDateTime UPDATED_AT_1 = UPDATED_AT.plusMinutes(1);
    private static final ZonedDateTime UPDATED_AT_2 = UPDATED_AT.plusMinutes(2);
    private static final ZonedDateTime UPDATED_AT_3 = UPDATED_AT.plusMinutes(3);
    private static final String ERROR_1_HASH = "hash-1";
    private static final String ERROR_2_HASH = "hash-2";
    final StreamErrorDetails STREAM_1_LISTENER_ERROR_1 = new StreamErrorDetails(
            randomUUID(), ERROR_1_HASH, "some-exception-message", empty(),
            "event-name", randomUUID(), STREAM_1_ID, 1L,
            new UtcClock().now(), "stack-trace",
            EVENT_LISTENER_COMPONENT, LISTING_SOURCE
    );
    final StreamErrorDetails STREAM_1_INDEXER_ERROR_1 = new StreamErrorDetails(
            randomUUID(), ERROR_1_HASH, "some-exception-message", empty(),
            "event-name", randomUUID(), STREAM_1_ID, 1L,
            new UtcClock().now(), "stack-trace",
            EVENT_INDEXER_COMPONENT, LISTING_SOURCE
    );
    final StreamErrorDetails STREAM_2_LISTENER_ERROR_1 = new StreamErrorDetails(
            randomUUID(), ERROR_1_HASH, "some-exception-message", empty(),
            "event-name", randomUUID(), STREAM_2_ID, 1L,
            new UtcClock().now(), "stack-trace",
            EVENT_LISTENER_COMPONENT, LISTING_SOURCE
    );
    final StreamErrorDetails STREAM_2_INDEXER_ERROR_2 = new StreamErrorDetails(
            randomUUID(), ERROR_2_HASH, "some-exception-message", empty(),
            "event-name", randomUUID(), STREAM_2_ID, 1L,
            new UtcClock().now(), "stack-trace",
            EVENT_INDEXER_COMPONENT, LISTING_SOURCE
    );
    final StreamErrorDetails STREAM_3_ERROR_1 = new StreamErrorDetails(
            randomUUID(), ERROR_1_HASH, "some-exception-message", empty(),
            "event-name", randomUUID(), STREAM_3_ID, 1L,
            new UtcClock().now(), "stack-trace",
            EVENT_LISTENER_COMPONENT, LISTING_SOURCE
    );

    @Mock
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    @InjectMocks
    private NewStreamStatusRepository newStreamStatusRepository;

    @Spy
    private UtcClock utcClock;

    @InjectMocks
    private StreamStatusErrorPersistence streamStatusErrorPersistence;

    @InjectMocks
    private StreamErrorHashPersistence streamErrorHashPersistence;

    @InjectMocks
    private StreamErrorDetailsPersistence streamErrorDetailsPersistence;

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

        newStreamStatusRepository.insertIfNotExists(STREAM_1_ID, LISTING_SOURCE, EVENT_LISTENER_COMPONENT, UPDATED_AT, false);
        newStreamStatusRepository.insertIfNotExists(STREAM_1_ID, LISTING_SOURCE, EVENT_INDEXER_COMPONENT, UPDATED_AT, false);
        newStreamStatusRepository.insertIfNotExists(STREAM_2_ID, LISTING_SOURCE, EVENT_LISTENER_COMPONENT, UPDATED_AT, false);
        newStreamStatusRepository.insertIfNotExists(STREAM_2_ID, LISTING_SOURCE, EVENT_INDEXER_COMPONENT, UPDATED_AT, false);
        newStreamStatusRepository.insertIfNotExists(STREAM_3_ID, LISTING_SOURCE, EVENT_LISTENER_COMPONENT, UPDATED_AT, false);
        newStreamStatusRepository.insertIfNotExists(STREAM_4_ID, LISTING_SOURCE, EVENT_LISTENER_COMPONENT, UPDATED_AT_1, false);
        newStreamStatusRepository.insertIfNotExists(STREAM_4_ID, LISTING_SOURCE, EVENT_INDEXER_COMPONENT, UPDATED_AT_2, false);
        newStreamStatusRepository.insertIfNotExists(STREAM_5_ID, LISTING_SOURCE, EVENT_LISTENER_COMPONENT, UPDATED_AT_3, false);

        insertEntriesToStreamErrorHash(ERROR_1_HASH, ERROR_2_HASH, viewStoreDataSource);

        //errors only for streams 1-3
        streamErrorDetailsPersistence.insert(STREAM_1_LISTENER_ERROR_1, viewStoreDataSource.getConnection());
        streamErrorDetailsPersistence.insert(STREAM_1_INDEXER_ERROR_1, viewStoreDataSource.getConnection());
        streamErrorDetailsPersistence.insert(STREAM_2_LISTENER_ERROR_1, viewStoreDataSource.getConnection());
        streamErrorDetailsPersistence.insert(STREAM_2_INDEXER_ERROR_2, viewStoreDataSource.getConnection());
        streamErrorDetailsPersistence.insert(STREAM_3_ERROR_1, viewStoreDataSource.getConnection());

        //stream_status updatedAt is changed by below calls, so results will be returned in the reverse order of below calls
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
        assertNotNull(streamStatus1.updatedAt());

        final StreamStatus streamStatus2 = streamStatuses.get(1);
        assertThat(streamStatus2.streamId(), is(STREAM_2_ID));
        assertThat(streamStatus2.latestKnownPosition(), is(0L));
        assertThat(streamStatus2.position(), is(0L));
        assertThat(streamStatus2.component(), is(EVENT_LISTENER_COMPONENT));
        assertThat(streamStatus2.source(), is(LISTING_SOURCE));
        assertThat(streamStatus2.isUpToDate(), is(false));
        assertThat(streamStatus2.streamErrorId().get(), is(STREAM_2_LISTENER_ERROR_1.id()));
        assertThat(streamStatus2.streamErrorPosition().get(), is(1L));
        assertNotNull(streamStatus2.updatedAt());

        final StreamStatus streamStatus3 = streamStatuses.get(2);
        assertThat(streamStatus3.streamId(), is(STREAM_1_ID));
        assertThat(streamStatus3.latestKnownPosition(), is(0L));
        assertThat(streamStatus3.position(), is(0L));
        assertThat(streamStatus3.component(), is(EVENT_INDEXER_COMPONENT));
        assertThat(streamStatus3.source(), is(LISTING_SOURCE));
        assertThat(streamStatus3.isUpToDate(), is(false));
        assertThat(streamStatus3.streamErrorId().get(), is(STREAM_1_INDEXER_ERROR_1.id()));
        assertThat(streamStatus3.streamErrorPosition().get(), is(1L));
        assertNotNull(streamStatus3.updatedAt());

        final StreamStatus streamStatus4 = streamStatuses.get(3);
        assertThat(streamStatus4.streamId(), is(STREAM_1_ID));
        assertThat(streamStatus4.latestKnownPosition(), is(0L));
        assertThat(streamStatus4.position(), is(0L));
        assertThat(streamStatus4.component(), is(EVENT_LISTENER_COMPONENT));
        assertThat(streamStatus4.source(), is(LISTING_SOURCE));
        assertThat(streamStatus4.isUpToDate(), is(false));
        assertThat(streamStatus4.streamErrorId().get(), is(STREAM_1_LISTENER_ERROR_1.id()));
        assertThat(streamStatus4.streamErrorPosition().get(), is(1L));
        assertNotNull(streamStatus4.updatedAt());
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
        assertNotNull(streamStatus1.updatedAt());

        final StreamStatus streamStatus2 = streamStatuses.get(1);
        assertThat(streamStatus2.streamId(), is(STREAM_1_ID));
        assertThat(streamStatus2.latestKnownPosition(), is(0L));
        assertThat(streamStatus2.position(), is(0L));
        assertThat(streamStatus2.component(), is(EVENT_LISTENER_COMPONENT));
        assertThat(streamStatus2.source(), is(LISTING_SOURCE));
        assertThat(streamStatus2.isUpToDate(), is(false));
        assertThat(streamStatus2.streamErrorId().get(), is(STREAM_1_LISTENER_ERROR_1.id()));
        assertThat(streamStatus2.streamErrorPosition().get(), is(1L));
        assertNotNull(streamStatus2.updatedAt());
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
        assertNotNull(streamStatus1.updatedAt());

        final StreamStatus streamStatus2 = streamStatuses.get(1);
        assertThat(streamStatus2.streamId(), is(STREAM_2_ID));
        assertThat(streamStatus2.latestKnownPosition(), is(0L));
        assertThat(streamStatus2.position(), is(0L));
        assertThat(streamStatus2.component(), is(EVENT_INDEXER_COMPONENT));
        assertThat(streamStatus2.source(), is(LISTING_SOURCE));
        assertThat(streamStatus2.isUpToDate(), is(false));
        assertThat(streamStatus2.streamErrorId().get(), is(STREAM_2_INDEXER_ERROR_2.id()));
        assertThat(streamStatus2.streamErrorPosition().get(), is(1L));
        assertNotNull(streamStatus2.updatedAt());

        final StreamStatus streamStatus3 = streamStatuses.get(2);
        assertThat(streamStatus3.streamId(), is(STREAM_2_ID));
        assertThat(streamStatus3.latestKnownPosition(), is(0L));
        assertThat(streamStatus3.position(), is(0L));
        assertThat(streamStatus3.component(), is(EVENT_LISTENER_COMPONENT));
        assertThat(streamStatus3.source(), is(LISTING_SOURCE));
        assertThat(streamStatus3.isUpToDate(), is(false));
        assertThat(streamStatus3.streamErrorId().get(), is(STREAM_2_LISTENER_ERROR_1.id()));
        assertThat(streamStatus3.streamErrorPosition().get(), is(1L));
        assertNotNull(streamStatus3.updatedAt());

        final StreamStatus streamStatus4 = streamStatuses.get(3);
        assertThat(streamStatus4.streamId(), is(STREAM_1_ID));
        assertThat(streamStatus4.latestKnownPosition(), is(0L));
        assertThat(streamStatus4.position(), is(0L));
        assertThat(streamStatus4.component(), is(EVENT_INDEXER_COMPONENT));
        assertThat(streamStatus4.source(), is(LISTING_SOURCE));
        assertThat(streamStatus4.isUpToDate(), is(false));
        assertThat(streamStatus4.streamErrorId().get(), is(STREAM_1_INDEXER_ERROR_1.id()));
        assertThat(streamStatus4.streamErrorPosition().get(), is(1L));
        assertNotNull(streamStatus4.updatedAt());

        final StreamStatus streamStatus5 = streamStatuses.get(4);
        assertThat(streamStatus5.streamId(), is(STREAM_1_ID));
        assertThat(streamStatus5.latestKnownPosition(), is(0L));
        assertThat(streamStatus5.position(), is(0L));
        assertThat(streamStatus5.component(), is(EVENT_LISTENER_COMPONENT));
        assertThat(streamStatus5.source(), is(LISTING_SOURCE));
        assertThat(streamStatus5.isUpToDate(), is(false));
        assertThat(streamStatus5.streamErrorId().get(), is(STREAM_1_LISTENER_ERROR_1.id()));
        assertThat(streamStatus5.streamErrorPosition().get(), is(1L));
        assertNotNull(streamStatus5.updatedAt());
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
        final StreamErrorDetails streamErrorDetails = new StreamErrorDetails(
                randomUUID(), error1Hash, "some-exception-message", empty(),
                "event-name", randomUUID(), stream1Id, position,
                new UtcClock().now(), "stack-trace",
                component, source
        );
        streamErrorDetailsPersistence.insert(streamErrorDetails, dataSource.getConnection());
    }
}
